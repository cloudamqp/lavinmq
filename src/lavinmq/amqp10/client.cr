require "openssl"
require "../client/client"
require "../client/channel"
require "../client/channel/consumer"
require "../connection_info"
require "../logger"
require "../rough_time"
require "../stats"
require "../event_type"
require "../auth/permission_cache"
require "../amqp/queue"
require "./address"
require "./protocol"
require "./message_codec"
require "./session"

module LavinMQ::AMQP10
  class Client < LavinMQ::Client
    include Stats
    include SortableJSON

    Log                 = LavinMQ::Log.for "amqp10.client"
    SERVER_CONTAINER_ID = "lavinmq"

    # The queue a receiving link subscribed to an exchange consumes from.
    # A temporary one is deleted with the link, as a dynamic queue is.
    record Subscription, queue : LavinMQ::AMQP::Queue, durable : Bool, shared : Bool, temporary : Bool

    getter vhost, user, name, connection_info, auth_mechanism, container_id
    getter max_frame_size : UInt32
    getter client_properties = LavinMQ::AMQP::Table.new

    @sessions = Hash(UInt16, Session).new
    # Channels of sessions we ended (e.g. closed via the management API) whose
    # end the peer has not answered yet; their frames are discarded meanwhile.
    @ending_sessions = Set(UInt16).new
    @ending_lock = Mutex.new(:checked)
    @running = true
    @write_lock = Mutex.new(:checked)
    @descriptor_reader = IO::Memory.new(Bytes.empty)
    @acl_write_cache = Auth::PermissionCache.new
    @last_recv = RoughTime.instant
    @last_sent = RoughTime.instant
    # idle-time-out negotiated with the peer (milliseconds), nil for none:
    #   @remote_idle_timeout — the peer's; we must send a frame within it
    #   @local_idle_timeout  — ours; we may drop the peer if it goes silent
    @remote_idle_timeout : UInt32?
    getter log : Logger
    # Open transactions by id, and the file their published bodies are kept
    # in until they are discharged; emptied once none are open.
    @transactions = Hash(UInt64, Transaction).new
    @last_transaction_id = 0_u64
    @transaction_body_file : File?

    rate_stats({"send_oct", "recv_oct"})

    # @max_frame_size is the limit for outgoing frames, @local_max_frame_size
    # ours, advertised in the Open and the limit for incoming ones.
    def initialize(@socket : IO,
                   @connection_info : ConnectionInfo,
                   @vhost : VHost,
                   @user : Auth::BaseUser,
                   @auth_mechanism : String,
                   @container_id : String,
                   @max_frame_size : UInt32,
                   @local_max_frame_size : UInt32,
                   @channel_max : UInt16,
                   remote_idle_timeout : UInt32?,
                   @local_idle_timeout : UInt32?,
                   @frame_reader : FrameReader)
      # A zero idle-time-out in the peer's Open means none (2.4.5)
      @remote_idle_timeout = remote_idle_timeout unless remote_idle_timeout.try(&.zero?)
      @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
      @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @connection_info.remote_address.to_s})
      @log = Logger.new(Log, @metadata)
    end

    def run : Nil
      @log.info { "AMQP 1.0 connection established for user=#{@user.name}" }
      case user = @user
      when Auth::OAuthUser
        user.on_expiration do
          close("token expired")
        end
      end
      read_loop
    end

    def channel_count : Int32
      @sessions.size
    end

    def each_channel(& : LavinMQ::Client::Channel ->) : Nil
      @sessions.each_value { |ch| yield ch }
    end

    def channels : Array(LavinMQ::Client::Channel)
      @sessions.values.map(&.as(LavinMQ::Client::Channel))
    end

    def channel?(id : UInt16) : LavinMQ::Client::Channel?
      @sessions[id]?
    end

    def client_name
      @name
    end

    def details_tuple
      {
        channels:          @sessions.size,
        connected_at:      @connected_at,
        type:              "network",
        channel_max:       @channel_max,
        frame_max:         @max_frame_size,
        timeout:           0,
        client_properties: @client_properties,
        vhost:             @vhost.name,
        user:              @user.name,
        protocol:          "AMQP 1.0",
        auth_mechanism:    @auth_mechanism,
        host:              @connection_info.local_address.address,
        port:              @connection_info.local_address.port,
        peer_host:         @connection_info.remote_address.address,
        peer_port:         @connection_info.remote_address.port,
        name:              @name,
        pid:               @name,
        ssl:               @connection_info.ssl?,
        tls_version:       @connection_info.ssl_version,
        cipher:            @connection_info.ssl_cipher,
        state:             state,
      }.merge(current_stats_details)
    end

    def to_json(json : JSON::Builder)
      details_tuple.merge(stats_details).to_json(json)
    end

    def connection_details
      {
        peer_host: @connection_info.remote_address.address,
        peer_port: @connection_info.remote_address.port,
        name:      @name,
      }
    end

    def state
      !@running ? "closed" : (@vhost.flow? ? "running" : "flow")
    end

    # A dynamic source is consumed from and a dynamic target published to (via
    # the default exchange), so that access is required as well as configure.
    def declare_dynamic_queue(*, consume : Bool) : LavinMQ::AMQP::Queue
      name = LavinMQ::AMQP::Queue.generate_name
      check_can_create_queue!(name)
      if consume
        check_can_consume!(name)
      elsif !@user.can_write?(@vhost.name, "", @acl_write_cache)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' not allowed to publish to exchange ''")
      end
      declare_exclusive_queue(name)
    end

    def close_dynamic_queue(q : LavinMQ::AMQP::Queue?) : Nil
      return unless q
      @exclusive_queues.delete(q)
      q.close
    end

    # Argument of a subscription queue holding the address it was made for,
    # telling it apart from other queues and giving back its source on lookup
    SUBSCRIPTION_SOURCE_ARGUMENT = "x-amqp10-source"

    # Subscribes a receiving link to an exchange, with a queue bound to it:
    #   - shared (the `shared` capability) or not: a shared subscription's queue
    #     is consumed by every link attached to it, an unshared one's by one
    #   - durable (expiry-policy never) or not: a durable subscription's queue
    #     outlives its links until unsubscribed, a non-durable unshared one is
    #     deleted with its link and a non-durable shared one with its last link
    #   - global (the `global` capability, with `shared`): the subscription is
    #     named by the link alone instead of by container-id and link
    # The names follow Qpid JMS, which appends `|<suffix>` to the subscription
    # name in the link names of shared subscriptions.
    def subscribe(link_name : String, address : String, exchange : String, binding_key : String,
                  durable : Bool, shared : Bool, global : Bool) : Subscription
      ex = @vhost.exchange?(exchange) || raise LinkError.new(ErrorCondition::NOT_FOUND, "exchange '#{exchange}' not found")
      unless @user.can_read?(@vhost.name, exchange)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' doesn't have read permissions to exchange '#{exchange}'")
      end
      unless durable || shared
        name = LavinMQ::AMQP::Queue.generate_name
        check_can_create_queue!(name)
        check_subscription_permissions!(name)
        q = declare_exclusive_queue(name)
        bind_subscription(q, ex, binding_key)
        return Subscription.new(q, durable: false, shared: false, temporary: true)
      end
      subscription_name = shared ? link_name.partition('|').first : link_name
      name = subscription_queue_name(subscription_name, durable, global && shared)
      check_subscription_permissions!(name)
      q = existing_subscription(name, address)
      unless q
        check_can_create_queue!(name)
        arguments = LavinMQ::AMQP::Table.new({SUBSCRIPTION_SOURCE_ARGUMENT => address})
        @vhost.declare_queue(name, durable, !durable, arguments)
        q = @vhost.queue?(name).as?(LavinMQ::AMQP::Queue) || raise LinkError.new(ErrorCondition::RESOURCE_DELETED, "queue '#{name}' was deleted")
      end
      bind_subscription(q, ex, binding_key)
      Subscription.new(q, durable: durable, shared: shared, temporary: false)
    end

    # The durable subscription a link attached without a source names, as
    # Qpid JMS attaches to unsubscribe: `<name>` is the subscription of this
    # container, `<name>|global` a global shared one.
    def find_subscription(link_name : String) : Tuple(LavinMQ::AMQP::Queue, String)?
      name = if global_name = link_name.rchop?("|global")
               subscription_queue_name(global_name, durable: true, global: true)
             else
               subscription_queue_name(link_name, durable: true, global: false)
             end
      q = @vhost.queue?(name).as?(LavinMQ::AMQP::Queue) || return
      address = q.arguments[SUBSCRIPTION_SOURCE_ARGUMENT]?.as?(String) || return
      check_can_consume!(name)
      {q, address}
    end

    # Deletes a durable subscription's queue once no link consumes from it.
    # Returns false, leaving it, while others still do.
    def unsubscribe(q : LavinMQ::AMQP::Queue) : Bool
      return false unless q.consumer_count.zero?
      @vhost.delete_queue(q.name)
      true
    end

    private def subscription_queue_name(subscription_name : String, durable : Bool, global : Bool) : String
      name = case {durable, global}
             when {true, false}  then "amqp10.sub.#{@container_id}.#{subscription_name}"
             when {true, true}   then "amqp10.gsub.#{subscription_name}"
             when {false, false} then "amqp10.vsub.#{@container_id}.#{subscription_name}"
             else                     "amqp10.gvsub.#{subscription_name}"
             end
      unless name.bytesize <= UInt8::MAX && NameValidator.valid_entity_name?(name)
        raise LinkError.new(ErrorCondition::INVALID_FIELD, "subscription name '#{subscription_name}' is too long or not printable ASCII")
      end
      name
    end

    # The subscription queue named, unless it is to be created: when there is
    # none, or it was made for another address and nothing consumes from it,
    # in which case it is deleted, as a subscription changing its topic is a
    # new one.
    private def existing_subscription(name : String, address : String) : LavinMQ::AMQP::Queue?
      q = @vhost.queue?(name) || return
      q = q.as?(LavinMQ::AMQP::Queue) || raise LinkError.new(ErrorCondition::RESOURCE_LOCKED, "queue '#{name}' is not a subscription")
      source = q.arguments[SUBSCRIPTION_SOURCE_ARGUMENT]?.as?(String)
      raise LinkError.new(ErrorCondition::RESOURCE_LOCKED, "queue '#{name}' is not a subscription") unless source
      return q if source == address
      unless q.consumer_count.zero?
        raise LinkError.new(ErrorCondition::RESOURCE_LOCKED, "subscription '#{name}' is in use for '#{source}'")
      end
      @vhost.delete_queue(name)
      nil
    end

    private def bind_subscription(q : LavinMQ::AMQP::Queue, ex : LavinMQ::Exchange, binding_key : String) : Nil
      @vhost.bind_queue(q.name, ex.name, binding_key)
    rescue err : LavinMQ::Exchange::AccessRefused
      raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, err.message || "binding refused")
    end

    # As a 0-9-1 queue.declare, basic.consume and queue.bind would require
    private def check_subscription_permissions!(name : String) : Nil
      unless @user.can_config?(@vhost.name, name)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' does not have permissions to queue '#{name}'")
      end
      check_can_consume!(name)
      unless @user.can_write?(@vhost.name, name)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' doesn't have write permissions to queue '#{name}'")
      end
    end

    private def check_can_create_queue!(name : String) : Nil
      raise LinkError.new(ErrorCondition::RESOURCE_LIMIT_EXCEEDED, "Server low on disk space, can not create queue") unless @vhost.flow?
      if @vhost.queue_limit_reached?
        raise LinkError.new(ErrorCondition::RESOURCE_LIMIT_EXCEEDED, "queue limit in vhost '#{@vhost.name}' is reached")
      end
      unless @user.can_config?(@vhost.name, name)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' does not have permissions to queue '#{name}'")
      end
    end

    private def check_can_consume!(name : String) : Nil
      unless @user.can_read?(@vhost.name, name)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' does not have permissions to queue '#{name}'")
      end
    end

    private def declare_exclusive_queue(name : String) : LavinMQ::AMQP::Queue
      frame = LavinMQ::AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, false, true, true, false, LavinMQ::AMQP::Table.new)
      @vhost.apply(frame)
      q = @vhost.queue(name).as(LavinMQ::AMQP::Queue)
      @exclusive_queues << q
      q.exclusive_owner = self
      q
    end

    def resolve_publish_target(address : String) : PublishAddress
      # An address outside the /queues/ and /exchanges/ schemes names no node we have.
      parsed = Address.parse_target(address) || raise LinkError.new(ErrorCondition::NOT_FOUND, "invalid target address #{address}")
      if parsed.exchange.empty?
        # As in 0-9-1, exclusive queues only refuse other connections'
        # consumers, so replies can be published to a requester's dynamic queue.
        @vhost.queue?(parsed.routing_key).as?(LavinMQ::AMQP::Queue) || raise LinkError.new(ErrorCondition::NOT_FOUND, "queue '#{parsed.routing_key}' not found")
      else
        ex = @vhost.exchange?(parsed.exchange) || raise LinkError.new(ErrorCondition::NOT_FOUND, "exchange '#{parsed.exchange}' not found")
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "Exchange '#{parsed.exchange}' is internal") if ex.internal?
      end
      unless @user.can_write?(@vhost.name, parsed.exchange, @acl_write_cache)
        raise LinkError.new(ErrorCondition::UNAUTHORIZED_ACCESS, "User '#{@user.name}' not allowed to publish to exchange '#{parsed.exchange}'")
      end
      parsed
    end

    def resolve_source(address : String) : LavinMQ::AMQP::Queue
      queue_name = Address.parse_source(address) || raise LinkError.new(ErrorCondition::NOT_FOUND, "invalid source address #{address}")
      q = @vhost.queue?(queue_name).as?(LavinMQ::AMQP::Queue) || raise LinkError.new(ErrorCondition::NOT_FOUND, "queue '#{queue_name}' not found")
      raise LinkError.new(ErrorCondition::RESOURCE_LOCKED, "Queue '#{q.name}' is exclusive") if queue_exclusive_to_other_client?(q)
      check_can_consume!(queue_name)
      q
    end

    # Open has no offered-capabilities field, so the frame is written here.
    # ANONYMOUS-RELAY tells clients such as Qpid JMS that a link without a
    # target address routes each message by its `to` property, SHARED-SUBS
    # that subscriptions may be shared.
    def send_open : Nil
      idle = @local_idle_timeout
      capabilities = [Value.symbol("ANONYMOUS-RELAY"), Value.symbol("SHARED-SUBS")]
      fields_size = Codec.string_size(SERVER_CONTAINER_ID) + 1 + Codec.uint_size(@local_max_frame_size) + 3 +
                    (idle ? Codec.uint_size(idle) : 1) + 2 + Codec.encoded_size(Value.array(capabilities))
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, 0_u16) do |io|
        Codec.write_descriptor(io, Descriptor::OPEN)
        Codec.write_list_header(io, fields_size, 8)
        Codec.write_string(io, SERVER_CONTAINER_ID)
        io.write_byte 0x40_u8 # hostname
        Codec.write_uint(io, @local_max_frame_size)
        io.write_byte 0x60_u8 # channel-max: ushort
        Codec.write_u16(io, @channel_max)
        idle ? Codec.write_uint(io, idle) : io.write_byte(0x40_u8)
        io.write_byte 0x40_u8 # outgoing-locales
        io.write_byte 0x40_u8 # incoming-locales
        Codec.write_array(io, capabilities)
      end
    end

    def send_begin(channel : UInt16) : Nil
      fields_size = 3 + Codec.uint_size(0_u32) + Codec.uint_size(DEFAULT_WINDOW) + Codec.uint_size(DEFAULT_WINDOW)
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, channel) do |io|
        Codec.write_descriptor(io, Descriptor::BEGIN)
        Codec.write_list_header(io, fields_size, 4)
        io.write_byte 0x60_u8 # remote-channel: ushort
        Codec.write_u16(io, channel)
        Codec.write_uint(io, 0_u32)
        Codec.write_uint(io, DEFAULT_WINDOW)
        Codec.write_uint(io, DEFAULT_WINDOW)
      end
    end

    def send_attach(session : Session, link : Link, source : Source?, target : (Target | Coordinator)?,
                    remote_attach : Attach? = nil) : Nil
      snd_mode, rcv_mode = settle_modes(link, remote_attach)
      write_attach(session, link.name, link.local_handle, link.role, snd_mode, rcv_mode, source, target, link.delivery_count)
    end

    # Each settle-mode field states the actual mode of the peer playing that
    # role: ours for the role we play, echoed for the role the peer plays.
    private def settle_modes(link : Link, remote_attach : Attach?) : Tuple(UInt8, UInt8)
      if link.role.receiver?
        {remote_attach.try(&.snd_settle_mode) || 0_u8, link.as?(ReceiverLink).try(&.settle_second?) ? 1_u8 : 0_u8}
      else
        {link.is_a?(SenderLink) && link.settled? ? 1_u8 : 0_u8, remote_attach.try(&.rcv_settle_mode) || 0_u8}
      end
    end

    # A refused attach is answered with one without source and target (2.6.3),
    # followed by a detach carrying the error.
    def send_rejected_attach(session : Session, remote_attach : Attach, local_handle : UInt32) : Nil
      role = remote_attach.role.sender? ? Role::Receiver : Role::Sender
      write_attach(session, remote_attach.name, local_handle, role,
        remote_attach.snd_settle_mode || 0_u8, remote_attach.rcv_settle_mode || 0_u8, nil, nil, 0_u32)
    end

    private def write_attach(session : Session, name : String, handle : UInt32, role : Role, snd_mode : UInt8,
                             rcv_mode : UInt8, source : Source?, target : (Target | Coordinator)?,
                             delivery_count : UInt32) : Nil
      fields_size = Codec.string_size(name) + Codec.uint_size(handle) + 1 + 2 + 2 +
                    (source.try(&.encoded_size) || 1) + (target.try(&.encoded_size) || 1)
      fields_count = 7
      if role.sender?
        fields_size += 1 + 1 + Codec.uint_size(delivery_count)
        fields_count = 10
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, session.id) do |io|
        Codec.write_descriptor(io, Descriptor::ATTACH)
        Codec.write_list_header(io, fields_size, fields_count)
        Codec.write_string(io, name)
        Codec.write_uint(io, handle)
        io.write_byte(role.receiver? ? 0x41_u8 : 0x42_u8)
        io.write_byte 0x50_u8
        io.write_byte snd_mode
        io.write_byte 0x50_u8
        io.write_byte rcv_mode
        source ? source.write_to(io) : io.write_byte(0x40_u8)
        target ? target.write_to(io) : io.write_byte(0x40_u8)
        if role.sender?
          io.write_byte 0x40_u8 # unsettled: null
          io.write_byte 0x40_u8 # incomplete-unsettled: null
          Codec.write_uint(io, delivery_count)
        end
      end
    end

    def send_detach(session : Session, handle : UInt32, closed = true, error : ErrorInfo? = nil) : Nil
      fields_size = Codec.uint_size(handle) + 1
      fields_count = 2
      if error
        fields_size += error.encoded_size
        fields_count = 3
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, session.id) do |io|
        Codec.write_descriptor(io, Descriptor::DETACH)
        Codec.write_list_header(io, fields_size, fields_count)
        Codec.write_uint(io, handle)
        io.write_byte(closed ? 0x41_u8 : 0x42_u8)
        error.write_to(io) if error
      end
    end

    def send_end(channel : UInt16, error : ErrorInfo? = nil) : Nil
      send_with_optional_error(Descriptor::END, channel, error)
    end

    # Server-initiated session end: the session is closed right away, like a
    # 0-9-1 channel closed via the management API, and the peer is told why.
    # The session stays registered until the peer answers with its own end.
    def end_session(session : Session, error : ErrorInfo) : Nil
      @ending_lock.synchronize { return unless @ending_sessions.add?(session.id) }
      session.close
      send_end(session.id, error)
    end

    private def ending_session?(channel : UInt16) : Bool
      @ending_lock.synchronize { @ending_sessions.includes?(channel) }
    end

    # The peer answered our end: the channel may be reused from now on.
    private def finish_ending_session(channel : UInt16) : Nil
      @ending_lock.synchronize { @ending_sessions.delete(channel) }
      @sessions.delete(channel).try &.close
    end

    def send_close(error : ErrorInfo? = nil) : Nil
      send_with_optional_error(Descriptor::CLOSE, 0_u16, error)
    ensure
      @running = false
    end

    # End and Close carry an optional error and nothing else
    private def send_with_optional_error(descriptor : UInt64, channel : UInt16, error : ErrorInfo?) : Nil
      if error
        fields_size = error.encoded_size
        frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
        send_frame(frame_size.to_u32, channel) do |io|
          Codec.write_descriptor(io, descriptor)
          Codec.write_list_header(io, fields_size, 1)
          error.write_to(io)
        end
      else
        send_frame((8 + 3 + 1).to_u32, channel) do |io|
          Codec.write_descriptor(io, descriptor)
          io.write_byte 0x45_u8 # empty list0
        end
      end
    end

    def send_flow(session : Session, link : Link, credit : UInt32, drain : Bool = false) : Nil
      write_locked do |io|
        TransferCodec.write_flow(io, session.id, session.next_incoming_id, session.incoming_window,
          session.next_outgoing_id, DEFAULT_WINDOW, link.local_handle, link.delivery_count, credit, drain)
      end
    end

    def send_session_flow(session : Session) : Nil
      write_locked do |io|
        TransferCodec.write_flow(io, session.id, session.next_incoming_id, session.incoming_window,
          session.next_outgoing_id, DEFAULT_WINDOW)
      end
    end

    # With a txn_id the outcome is sent in a transactional-state.
    def send_disposition(session : Session, first : UInt32, outcome : Outcome, settled = true,
                         last : UInt32? = nil, txn_id : Bytes? = nil, error : ErrorInfo? = nil) : Nil
      write_locked do |io|
        TransferCodec.write_disposition(io, session.id, first, outcome, settled, Role::Receiver, last, txn_id, error)
      end
    end

    def send_declared(session : Session, delivery_id : UInt32, txn_id : Bytes) : Nil
      write_locked { |io| TransferCodec.write_declared(io, session.id, delivery_id, txn_id) }
    end

    def declare_transaction(coordinator : CoordinatorLink) : Transaction
      id = @last_transaction_id &+= 1
      @transactions[id] = Transaction.new(id, coordinator, transaction_body_file)
    end

    def transaction?(txn_id : Bytes) : Transaction?
      return unless txn_id.size == 8
      @transactions[IO::ByteFormat::NetworkEndian.decode(UInt64, txn_id)]?
    end

    def end_transaction(transaction : Transaction) : Nil
      @transactions.delete(transaction.id)
      if @transactions.empty?
        @transaction_body_file.try &.truncate(0)
      end
    end

    # Rolls back the transactions open on a coordinator link that is closing
    def rollback_transactions(coordinator : CoordinatorLink) : Nil
      @transactions.each_value.select(&.coordinator.same?(coordinator)).to_a.each do |transaction|
        transaction.rollback
        end_transaction(transaction)
      end
    end

    private def transaction_body_file : File
      @transaction_body_file ||= File.tempfile("amqp10-transaction.", nil, dir: @vhost.data_dir).tap do |f|
        f.sync = true
        f.read_buffering = false
        f.delete
      end
    end

    # Sender-side settlement of deliveries a rcv-settle-mode second receiver
    # has applied an outcome to but is still holding until we settle.
    def send_settlement(session : Session, first : UInt32, last : UInt32?, outcome : Outcome) : Nil
      write_locked { |io| TransferCodec.write_disposition(io, session.id, first, outcome, true, Role::Sender, last) }
    end

    # Yields the socket under the write lock; the block returns the bytes written
    private def write_locked(& : IO -> UInt64) : Nil
      bytes = @write_lock.synchronize { yield @socket }
      add_send_bytes(bytes)
    end

    def send_transfer(session : Session, link : SenderLink, msg : BytesMessage,
                      sp : SegmentPosition, settled : Bool, redelivered = false) : Bool
      # Walks the message's headers, so done before taking the lock
      sizes = MessageCodec.section_sizes(msg, redelivered)
      @write_lock.synchronize do
        # Assign the delivery-id and record the unacked entry under the lock so
        # ids and unacked stay ordered even if two sender links deliver
        # concurrently.
        delivery_id = session.take_delivery_id
        tag = link.delivery_tag_buffer
        IO::ByteFormat::NetworkEndian.encode(delivery_id.to_u64, tag)
        link.record_unacked(delivery_id, sp, sizes.annotations) unless settled
        bytes, _frames = MessageCodec.write_transfer(@socket, session.id, link.local_handle,
          delivery_id, tag, msg, sizes, @max_frame_size, settled) do |more|
          session.advance_outgoing(1_u32)
          wait_for_session_window(session, link) if more
        end
        add_send_bytes(bytes)
      end
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error | ProtocolError
      @log.debug { "Lost AMQP 1.0 connection while sending transfer: #{ex.inspect}" }
      close_socket
      false
    end

    # The peer's session window can run out partway through a delivery; the
    # remaining frames wait for its flow. The write lock is released meanwhile,
    # as the read loop needs it to answer frames, and the window is checked
    # again under the lock, which every transfer is written under.
    private def wait_for_session_window(session : Session, link : SenderLink) : Nil
      until session.remote_window_open?
        @socket.flush
        @write_lock.unlock
        begin
          link.wait_for_remote_window
        ensure
          @write_lock.lock
        end
      end
    end

    def flush : Nil
      @write_lock.synchronize { @socket.flush }
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost AMQP 1.0 connection while flushing: #{ex.inspect}" } unless closed?
      close_socket
    end

    private def send_frame(frame_size : UInt32, channel : UInt16, &) : Nil
      @write_lock.synchronize do
        FrameWriter.write_frame_header(@socket, frame_size, AMQP_FRAME_TYPE, channel)
        yield @socket
        @socket.flush
      end
      add_send_bytes(frame_size.to_u64)
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost AMQP 1.0 connection while sending: #{ex.inspect}" } unless closed?
      close_socket
    end

    private def add_send_bytes(bytes : UInt64) : Nil
      @last_sent = RoughTime.instant
      @send_oct_count.add(bytes, :relaxed)
      @vhost.add_send_bytes(bytes)
    end

    private def read_loop
      reader = connection_frame_reader
      configure_idle_timeout
      @last_recv = RoughTime.instant
      while @running
        begin
          frame = reader.read
        rescue IO::TimeoutError
          handle_idle_timeout || break
          next
        end
        @last_recv = RoughTime.instant
        recv_bytes = 8_u64 + frame.body.bytesize
        @recv_oct_count.add(recv_bytes, :relaxed)
        @vhost.add_recv_bytes(recv_bytes)
        process_frame(frame)
        # A peer that keeps sending (e.g. pre-settled publishes) never lets the
        # read time out, so the keepalive must also be checked here.
        send_empty_frame if keepalive_due?
      end
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost AMQP 1.0 connection while reading: #{ex.inspect}" } unless closed?
    rescue ex : DecodeError | ProtocolError | OverflowError
      @log.warn { "AMQP 1.0 protocol error: #{ex.message}" }
      send_close(ErrorInfo.new(ErrorCondition::DECODE_ERROR, ex.message)) unless closed?
    rescue ex
      @log.error(exception: ex) { "Unexpected AMQP 1.0 error: #{ex.message}" }
      send_close(ErrorInfo.new(ErrorCondition::INTERNAL_ERROR, "internal error")) unless closed?
    ensure
      cleanup
      close_socket
      @log.info { "AMQP 1.0 connection disconnected for user=#{@user.name} duration=#{duration}" }
    end

    # Keeps using the reader the Open was read with when its buffer can hold
    # our max-frame-size, which it always can unless frame_max is 0
    # (unlimited) and the reader was sized to the minimum.
    private def connection_frame_reader : FrameReader
      reader = @frame_reader
      reader = FrameReader.new(@socket, @local_max_frame_size) if reader.buffer_size < @local_max_frame_size
      reader.max_frame_size = @local_max_frame_size
      reader
    end

    private def configure_idle_timeout : Nil
      socket = @socket
      return unless socket.responds_to?(:"read_timeout=")
      # Replaces the short handshake timeout the connection factory set: either
      # with the idle check interval, or with none when no idle-timeout was
      # negotiated in either direction.
      socket.read_timeout = idle_check_interval
    end

    # How often the read loop should wake to send a keepalive and/or check the
    # peer's liveness; nil when no idle-timeout was negotiated in either direction.
    private def idle_check_interval : Time::Span?
      remote, local = @remote_idle_timeout, @local_idle_timeout
      timeout = (remote && local ? Math.min(remote, local) : remote || local) || return
      Math.max(timeout // 2, 1_u32).milliseconds
    end

    # Returns false when the peer has gone silent past our advertised idle-timeout
    # (with grace); otherwise sends an empty keepalive when the peer expects one.
    private def handle_idle_timeout : Bool
      if (l = @local_idle_timeout) && (RoughTime.instant - @last_recv) > (l + l // 2).milliseconds
        @log.info { "AMQP 1.0 idle timeout, no frames received for #{l} ms" }
        return false
      end
      send_empty_frame if keepalive_due?
      true
    end

    # The peer drops us if we stay silent for its idle-time-out, so a keepalive
    # is due once half of it has passed since the last frame we sent.
    private def keepalive_due? : Bool
      r = @remote_idle_timeout || return false
      RoughTime.instant - @last_sent >= (r // 2).milliseconds
    end

    private def send_empty_frame : Nil
      @write_lock.synchronize do
        FrameWriter.write_frame_header(@socket, 8_u32, AMQP_FRAME_TYPE, 0_u16)
        @socket.flush
      end
      add_send_bytes(8_u64)
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost AMQP 1.0 connection while sending keepalive: #{ex.inspect}" } unless closed?
      close_socket
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def process_frame(frame : Frame) : Nil
      raise DecodeError.new("unexpected SASL frame after SASL negotiation") unless frame.type == AMQP_FRAME_TYPE
      return if frame.body.empty? # empty (idle-timeout keepalive) frame

      code = peek_descriptor_code(frame.body)
      if ending_session?(frame.channel)
        # Frames the peer sent before it saw our end are dropped; its end
        # completes the exchange and must not be answered with another one.
        finish_ending_session(frame.channel) if code == Descriptor::END
        return
      end

      case code
      when Descriptor::TRANSFER
        reader = frame.body_reader
        transfer = TransferCodec.read_transfer(reader)
        session(frame.channel).transfer(transfer, reader.peek)
        return
      when Descriptor::DISPOSITION
        session(frame.channel).disposition(TransferCodec.read_disposition(frame.body_reader))
        return
      when Descriptor::FLOW
        session(frame.channel).flow(TransferCodec.read_flow(frame.body_reader))
        return
      end

      reader = frame.body_reader
      value = Codec.decode(reader)
      described = value.described? || raise DecodeError.new("expected performative")
      case described.descriptor_code?
      when Descriptor::BEGIN
        open_session(frame.channel, Begin.from_value(value))
      when Descriptor::ATTACH
        session(frame.channel).attach(Attach.from_value(value))
      when Descriptor::DETACH
        session(frame.channel).detach(Detach.from_value(value))
      when Descriptor::END
        @sessions.delete(frame.channel).try &.close
        send_end(frame.channel)
      when Descriptor::CLOSE
        send_close unless closed?
        @running = false
      else
        raise ProtocolError.new("unsupported performative #{described.descriptor_code?}")
      end
    end

    private def peek_descriptor_code(body : Bytes) : UInt64?
      Codec.read_descriptor_code(@descriptor_reader.reset(body))
    rescue DecodeError | IO::EOFError
      nil
    end

    private def open_session(channel : UInt16, begin_frame : Begin) : Nil
      if channel > @channel_max
        raise ProtocolError.new("channel #{channel} exceeds the advertised channel-max #{@channel_max}")
      end
      if @sessions.has_key?(channel)
        raise ProtocolError.new("session already begun on channel #{channel}")
      end
      session = Session.new(self, channel, begin_frame)
      @sessions[channel] = session
      send_begin(channel)
      @vhost.event_tick(EventType::ChannelCreated)
    end

    private def session(channel : UInt16) : Session
      @sessions[channel]? || raise ProtocolError.new("session #{channel} not begun")
    end

    def close(reason = "Connection closed", timeout : Time::Span = 5.seconds)
      @log.info { "Closing AMQP 1.0 connection: #{reason}" }
      socket = @socket
      if socket.responds_to?(:"write_timeout=")
        socket.write_timeout = timeout
        socket.read_timeout = timeout
      end
      send_close(ErrorInfo.new(ErrorCondition::INTERNAL_ERROR, reason)) unless closed?
      spawn(name: "AMQP10::Client#close timeout #{@connection_info.remote_address}") do
        sleep timeout
        close_socket
      end
    end

    def force_close
      close_socket
    end

    def closed?
      !@running
    end

    private def cleanup
      @running = false
      @sessions.each_value &.close
      @sessions.clear
      @transaction_body_file.try &.close
      close_exclusive_queues
      case user = @user
      when Auth::OAuthUser
        user.cleanup
      end
    end

    private def close_socket
      @running = false
      @socket.close
    rescue ex
      @log.debug { "#{ex.inspect} when closing AMQP 1.0 socket" }
    end
  end
end
