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
require "../observable"
require "../amqp/queue/event"
require "../amqp/queue"
require "./address"
require "./frame"
require "./message_codec"
require "./slice_io"
require "./transfer_codec"
require "./types"

module LavinMQ::AMQP10
  class Client < LavinMQ::Client
    include Stats
    include SortableJSON
    include Observer(LavinMQ::QueueEvent)

    Log                 = LavinMQ::Log.for "amqp10.client"
    SERVER_CONTAINER_ID = "lavinmq"

    getter vhost, user, name, connection_info, auth_mechanism
    getter max_frame_size : UInt32
    getter client_properties = LavinMQ::AMQP::Table.new

    @connected_at = RoughTime.unix_ms
    @sessions = Hash(UInt16, Session).new
    @exclusive_queues = Array(LavinMQ::AMQP::Queue).new
    @running = true
    @write_lock = Mutex.new(:checked)
    @descriptor_reader = SliceReader.new

    rate_stats({"send_oct", "recv_oct"})

    def initialize(@socket : IO,
                   @connection_info : ConnectionInfo,
                   @vhost : VHost,
                   @user : Auth::BaseUser,
                   @auth_mechanism : String,
                   @max_frame_size : UInt32)
      @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
      @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @connection_info.remote_address.to_s})
      @log = Logger.new(Log, @metadata)
      @vhost.add_connection(self)
      @log.info { "AMQP 1.0 connection established for user=#{@user.name}" }
      spawn read_loop, name: "AMQP10::Client#read_loop #{@connection_info.remote_address}"
    end

    def on(event : LavinMQ::QueueEvent, data : Object?)
      @exclusive_queues.delete(data) if event.deleted? && data.is_a?(LavinMQ::AMQP::Queue)
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
        channel_max:       0,
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

    def queue_exclusive_to_other_client?(q : LavinMQ::AMQP::Queue)
      q.exclusive? && !@exclusive_queues.includes?(q)
    end

    def declare_dynamic_queue : LavinMQ::AMQP::Queue
      raise ProtocolError.new("Server low on disk space, can not create queue") unless @vhost.flow?
      if @vhost.max_queues.try { |max| @vhost.queues_size >= max }
        raise ProtocolError.new("queue limit in vhost '#{@vhost.name}' is reached")
      end

      name = LavinMQ::AMQP::Queue.generate_name
      unless @user.can_config?(@vhost.name, name)
        raise ProtocolError.new("User '#{@user.name}' does not have permissions to queue '#{name}'")
      end
      frame = LavinMQ::AMQP::Frame::Queue::Declare.new(0_u16, 0_u16, name, false, false, true, true, false, LavinMQ::AMQP::Table.new)
      @vhost.apply(frame)
      q = @vhost.queue(name).as(LavinMQ::AMQP::Queue)
      @exclusive_queues << q
      q.register_observer(self)
      q
    end

    def close_dynamic_queue(q : LavinMQ::AMQP::Queue?) : Nil
      return unless q
      @exclusive_queues.delete(q)
      q.close
    end

    def resolve_publish_target(address : String) : PublishAddress
      parsed = Address.parse_target(address) || raise ProtocolError.new("invalid target address #{address}")
      if parsed.exchange.empty?
        q = @vhost.queue?(parsed.routing_key).as?(LavinMQ::AMQP::Queue) || raise ProtocolError.new("queue '#{parsed.routing_key}' not found")
        raise ProtocolError.new("Queue '#{q.name}' is exclusive") if queue_exclusive_to_other_client?(q)
      else
        ex = @vhost.exchange?(parsed.exchange) || raise ProtocolError.new("exchange '#{parsed.exchange}' not found")
        raise ProtocolError.new("Exchange '#{parsed.exchange}' is internal") if ex.internal?
      end
      unless @user.can_write?(@vhost.name, parsed.exchange)
        raise ProtocolError.new("User '#{@user.name}' not allowed to publish to exchange '#{parsed.exchange}'")
      end
      parsed
    end

    def resolve_source(address : String) : LavinMQ::AMQP::Queue
      queue_name = Address.parse_source(address) || raise ProtocolError.new("invalid source address #{address}")
      q = @vhost.queue?(queue_name).as?(LavinMQ::AMQP::Queue) || raise ProtocolError.new("queue '#{queue_name}' not found")
      raise ProtocolError.new("Queue '#{q.name}' is exclusive") if queue_exclusive_to_other_client?(q)
      unless @user.can_read?(@vhost.name, queue_name)
        raise ProtocolError.new("User '#{@user.name}' does not have permissions to queue '#{queue_name}'")
      end
      q
    end

    def send_open : Nil
      fields = Array(Value).new(3)
      fields << Value.string(SERVER_CONTAINER_ID)
      fields << Value.null
      fields << Value.uint(@max_frame_size)
      send_performative(0_u16, Descriptor::OPEN, fields)
    end

    def send_begin(channel : UInt16) : Nil
      fields = Array(Value).new(4)
      fields << Value.ushort(channel)
      fields << Value.uint(0_u32)
      fields << Value.uint(DEFAULT_WINDOW)
      fields << Value.uint(DEFAULT_WINDOW)
      send_performative(channel, Descriptor::BEGIN, fields)
    end

    def send_attach(session : Session, link : Link, source : Source?, target : Target?, remote_attach : Attach? = nil) : Nil
      fields = Array(Value).new(link.role.sender? ? 10 : 7)
      fields << Value.string(link.name)
      fields << Value.uint(link.local_handle)
      fields << Value.bool(link.role.receiver?)
      fields << Value.ubyte(remote_attach.try(&.snd_settle_mode) || 0_u8)
      fields << Value.ubyte(remote_attach.try(&.rcv_settle_mode) || 0_u8)
      fields << (source.try(&.to_value) || Value.null)
      fields << (target.try(&.to_value) || Value.null)
      if link.role.sender?
        fields << Value.null
        fields << Value.null
        fields << Value.uint(link.delivery_count)
      end
      send_performative(session.id, Descriptor::ATTACH, fields)
    end

    def send_rejected_attach(session : Session, remote_attach : Attach, local_handle : UInt32) : Nil
      local_role_receiver = remote_attach.role.sender?
      fields = Array(Value).new(local_role_receiver ? 7 : 10)
      fields << Value.string(remote_attach.name)
      fields << Value.uint(local_handle)
      fields << Value.bool(local_role_receiver)
      fields << Value.ubyte(remote_attach.snd_settle_mode || 0_u8)
      fields << Value.ubyte(remote_attach.rcv_settle_mode || 0_u8)
      fields << Value.null
      fields << Value.null
      unless local_role_receiver
        fields << Value.null
        fields << Value.null
        fields << Value.uint(0_u32)
      end
      send_performative(session.id, Descriptor::ATTACH, fields)
    end

    def send_detach(session : Session, handle : UInt32, closed = true, error : ErrorInfo? = nil) : Nil
      fields = Array(Value).new(error ? 3 : 2)
      fields << Value.uint(handle)
      fields << Value.bool(closed)
      fields << error.to_value if error
      send_performative(session.id, Descriptor::DETACH, fields)
    end

    def send_end(channel : UInt16) : Nil
      send_performative(channel, Descriptor::END, Array(Value).new)
    end

    def send_close(error : ErrorInfo? = nil) : Nil
      fields = Array(Value).new(error ? 1 : 0)
      fields << error.to_value if error
      send_performative(0_u16, Descriptor::CLOSE, fields)
    ensure
      @running = false
    end

    def send_flow(session : Session, link : SenderLink | ReceiverLink, credit : UInt32, drain : Bool = false) : Nil
      bytes = @write_lock.synchronize do
        TransferCodec.write_flow(@socket, session.id, session.next_incoming_id, session.incoming_window,
          session.next_outgoing_id, DEFAULT_WINDOW, link.local_handle, link.delivery_count, credit, drain)
      end
      add_send_bytes(bytes)
    end

    def send_session_flow(session : Session) : Nil
      bytes = @write_lock.synchronize do
        TransferCodec.write_flow(@socket, session.id, session.next_incoming_id, session.incoming_window,
          session.next_outgoing_id, DEFAULT_WINDOW)
      end
      add_send_bytes(bytes)
    end

    def send_disposition(session : Session, first : UInt32, outcome : Outcome) : Nil
      @write_lock.synchronize do
        TransferCodec.write_disposition(@socket, session.id, first, outcome)
      end
      add_send_bytes(32_u64)
    end

    def send_transfer(session : Session, link : SenderLink, delivery_id : UInt32, delivery_tag : Bytes, msg : BytesMessage) : Bool
      bytes = @write_lock.synchronize do
        TransferCodec.write_transfer(@socket, session.id, link.local_handle, delivery_id, delivery_tag, msg, @max_frame_size)
      end
      add_send_bytes(bytes)
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error | ProtocolError
      @log.debug { "Lost AMQP 1.0 connection while sending transfer: #{ex.inspect}" }
      close_socket
      false
    end

    private def send_performative(channel : UInt16, code : UInt64, fields : Array(Value)) : Nil
      body_size = Codec.described_list_size(code, fields)
      @write_lock.synchronize do
        FrameWriter.write_frame_header(@socket, (8 + body_size).to_u32, AMQP_FRAME_TYPE, channel)
        Codec.write_described_list(@socket, code, fields)
        @socket.flush
      end
      add_send_bytes(8_u64 + body_size)
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "Lost AMQP 1.0 connection while sending: #{ex.inspect}" } unless closed?
      close_socket
    end

    private def add_send_bytes(bytes : UInt64) : Nil
      @send_oct_count.add(bytes, :relaxed)
      @vhost.add_send_bytes(bytes)
    end

    private def read_loop
      reader = FrameReader.new(@socket, @max_frame_size)
      while @running
        frame = reader.read
        recv_bytes = 8_u64 + frame.body.bytesize
        @recv_oct_count.add(recv_bytes, :relaxed)
        @vhost.add_recv_bytes(recv_bytes)
        process_frame(frame)
      end
    rescue ex : IO::Error | IO::TimeoutError | OpenSSL::SSL::Error
      @log.debug { "Lost AMQP 1.0 connection while reading: #{ex.inspect}" } unless closed?
    rescue ex : DecodeError | ProtocolError
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

    private def process_frame(frame : Frame) : Nil
      raise DecodeError.new("unexpected SASL frame after SASL negotiation") unless frame.type == AMQP_FRAME_TYPE

      case peek_descriptor_code(frame.body)
      when Descriptor::TRANSFER
        reader = frame.body_reader
        transfer = TransferCodec.read_transfer(reader)
        session(frame.channel).transfer(transfer, reader.remaining_slice)
        return
      when Descriptor::DISPOSITION
        session(frame.channel).disposition(TransferCodec.read_disposition(frame.body_reader))
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
      when Descriptor::FLOW
        session(frame.channel).flow(Flow.from_value(value))
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
      MessageCodec.read_descriptor_code(@descriptor_reader.reset(body))
    rescue DecodeError | IO::EOFError
      nil
    end

    private def open_session(channel : UInt16, begin_frame : Begin) : Nil
      if @sessions.has_key?(channel)
        raise ProtocolError.new("session already begun on channel #{channel}")
      end
      session = Session.new(self, channel)
      @sessions[channel] = session
      send_begin(channel)
      @vhost.event_tick(EventType::ChannelCreated)
    end

    private def session(channel : UInt16) : Session
      @sessions[channel]? || raise ProtocolError.new("session #{channel} not begun")
    end

    private def duration
      ms = RoughTime.unix_ms - @connected_at
      Time::Span.new(seconds: (ms / 1000).round.to_i)
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
      return unless @running
    ensure
      @running = false
      @sessions.each_value &.close
      @sessions.clear
      @exclusive_queues.dup.each(&.close)
      @exclusive_queues.clear
      @vhost.rm_connection(self)
    end

    private def close_socket
      @running = false
      @socket.close
    rescue ex
      @log.debug { "#{ex.inspect} when closing AMQP 1.0 socket" }
    end
  end
end
