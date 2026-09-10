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
require "./io_memory_reset"
require "./message_codec"
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
    @descriptor_reader = IO::Memory.new(Bytes.empty)
    @acl_write_cache = Auth::PermissionCache.new
    @last_recv = RoughTime.instant
    # idle-time-out negotiated with the peer (milliseconds):
    #   @remote_idle_timeout — the peer's; we must send a frame within it
    #   @local_idle_timeout  — ours; we may drop the peer if it goes silent
    @remote_idle_timeout : UInt32?
    @local_idle_timeout : UInt32?

    rate_stats({"send_oct", "recv_oct"})

    def initialize(@socket : IO,
                   @connection_info : ConnectionInfo,
                   @vhost : VHost,
                   @user : Auth::BaseUser,
                   @auth_mechanism : String,
                   @max_frame_size : UInt32,
                   @remote_idle_timeout : UInt32? = nil,
                   @local_idle_timeout : UInt32? = nil)
      @name = "#{@connection_info.remote_address} -> #{@connection_info.local_address}"
      @metadata = ::Log::Metadata.new(nil, {vhost: @vhost.name, address: @connection_info.remote_address.to_s})
      @log = Logger.new(Log, @metadata)
    end

    def log : Logger
      @log
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
      if @vhost.queue_limit_reached?
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
      unless @user.can_write?(@vhost.name, parsed.exchange, @acl_write_cache)
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
      fields_size = Codec.string_size(SERVER_CONTAINER_ID) + 1 + Codec.uint_size(@max_frame_size)
      fields_count = 3
      if idle = @local_idle_timeout
        fields_size += 1 + Codec.uint_size(idle)
        fields_count = 5
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, 0_u16) do |io|
        Codec.write_descriptor(io, Descriptor::OPEN)
        Codec.write_list_header(io, fields_size, fields_count)
        Codec.write_string(io, SERVER_CONTAINER_ID)
        io.write_byte 0x40_u8 # hostname: null
        Codec.write_uint(io, @max_frame_size)
        if idle = @local_idle_timeout
          io.write_byte 0x40_u8 # channel-max: null
          Codec.write_uint(io, idle)
        end
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

    def send_attach(session : Session, link : Link, source : Source?, target : Target?, remote_attach : Attach? = nil) : Nil
      snd_mode = remote_attach.try(&.snd_settle_mode) || 0_u8
      rcv_mode = remote_attach.try(&.rcv_settle_mode) || 0_u8
      fields_size = Codec.string_size(link.name) + Codec.uint_size(link.local_handle) + 1 + 2 + 2 +
                    (source.try(&.encoded_size) || 1) + (target.try(&.encoded_size) || 1)
      fields_count = 7
      if link.role.sender?
        fields_size += 1 + 1 + Codec.uint_size(link.delivery_count)
        fields_count = 10
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, session.id) do |io|
        Codec.write_descriptor(io, Descriptor::ATTACH)
        Codec.write_list_header(io, fields_size, fields_count)
        Codec.write_string(io, link.name)
        Codec.write_uint(io, link.local_handle)
        io.write_byte(link.role.receiver? ? 0x41_u8 : 0x42_u8)
        io.write_byte 0x50_u8
        io.write_byte snd_mode
        io.write_byte 0x50_u8
        io.write_byte rcv_mode
        source ? source.write_to(io) : io.write_byte(0x40_u8)
        target ? target.write_to(io) : io.write_byte(0x40_u8)
        if link.role.sender?
          io.write_byte 0x40_u8 # unsettled: null
          io.write_byte 0x40_u8 # incomplete-unsettled: null
          Codec.write_uint(io, link.delivery_count)
        end
      end
    end

    def send_rejected_attach(session : Session, remote_attach : Attach, local_handle : UInt32) : Nil
      local_role_receiver = remote_attach.role.sender?
      fields_size = Codec.string_size(remote_attach.name) + Codec.uint_size(local_handle) + 1 + 2 + 2 + 1 + 1
      fields_count = 7
      unless local_role_receiver
        fields_size += 1 + 1 + Codec.uint_size(0_u32)
        fields_count = 10
      end
      frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
      send_frame(frame_size.to_u32, session.id) do |io|
        Codec.write_descriptor(io, Descriptor::ATTACH)
        Codec.write_list_header(io, fields_size, fields_count)
        Codec.write_string(io, remote_attach.name)
        Codec.write_uint(io, local_handle)
        io.write_byte(local_role_receiver ? 0x41_u8 : 0x42_u8)
        io.write_byte 0x50_u8
        io.write_byte(remote_attach.snd_settle_mode || 0_u8)
        io.write_byte 0x50_u8
        io.write_byte(remote_attach.rcv_settle_mode || 0_u8)
        io.write_byte 0x40_u8 # source: null
        io.write_byte 0x40_u8 # target: null
        unless local_role_receiver
          io.write_byte 0x40_u8 # unsettled: null
          io.write_byte 0x40_u8 # incomplete-unsettled: null
          Codec.write_uint(io, 0_u32)
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

    def send_end(channel : UInt16) : Nil
      send_frame((8 + 3 + 1).to_u32, channel) do |io|
        Codec.write_descriptor(io, Descriptor::END)
        io.write_byte 0x45_u8 # empty list0
      end
    end

    def send_close(error : ErrorInfo? = nil) : Nil
      if error
        fields_size = error.encoded_size
        frame_size = 8 + 3 + Codec.list_header_size(fields_size) + fields_size
        send_frame(frame_size.to_u32, 0_u16) do |io|
          Codec.write_descriptor(io, Descriptor::CLOSE)
          Codec.write_list_header(io, fields_size, 1)
          error.write_to(io)
        end
      else
        send_frame((8 + 3 + 1).to_u32, 0_u16) do |io|
          Codec.write_descriptor(io, Descriptor::CLOSE)
          io.write_byte 0x45_u8 # empty list0
        end
      end
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

    def send_transfer(session : Session, link : SenderLink, msg : BytesMessage,
                      sp : SegmentPosition, settled : Bool) : Bool
      @write_lock.synchronize do
        # Assign the delivery-id (== transfer-id of the first frame) and record
        # the unacked entry under the lock so ids and unacked stay ordered even
        # if two sender links deliver concurrently.
        delivery_id = session.next_outgoing_id
        tag = link.delivery_tag_buffer
        IO::ByteFormat::NetworkEndian.encode(delivery_id.to_u64, tag)
        link.record_unacked(delivery_id, sp) unless settled
        bytes, frames = TransferCodec.write_transfer(@socket, session.id, link.local_handle,
          delivery_id, tag, msg, @max_frame_size, settled)
        session.advance_outgoing(frames)
        add_send_bytes(bytes)
      end
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error | ProtocolError
      @log.debug { "Lost AMQP 1.0 connection while sending transfer: #{ex.inspect}" }
      close_socket
      false
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
      @send_oct_count.add(bytes, :relaxed)
      @vhost.add_send_bytes(bytes)
    end

    private def read_loop
      reader = FrameReader.new(@socket, @max_frame_size)
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

    private def configure_idle_timeout : Nil
      socket = @socket
      return unless socket.responds_to?(:"read_timeout=")
      if interval = idle_check_interval
        socket.read_timeout = interval
      end
    end

    # How often the read loop should wake to send a keepalive and/or check the
    # peer's liveness; nil when no idle-timeout was negotiated in either direction.
    private def idle_check_interval : Time::Span?
      intervals = [] of Int64
      if r = @remote_idle_timeout
        intervals << (r // 2).to_i64 if r > 0
      end
      if l = @local_idle_timeout
        intervals << (l // 2).to_i64 if l > 0
      end
      return if intervals.empty?
      Math.max(intervals.min, 1_i64).milliseconds
    end

    # Returns false when the peer has gone silent past our advertised idle-timeout
    # (with grace); otherwise sends an empty keepalive when the peer expects one.
    private def handle_idle_timeout : Bool
      if l = @local_idle_timeout
        if l > 0 && (RoughTime.instant - @last_recv) > (l + l // 2).milliseconds
          @log.info { "AMQP 1.0 idle timeout, no frames received for #{l} ms" }
          return false
        end
      end
      send_empty_frame if (r = @remote_idle_timeout) && r > 0
      true
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

      case peek_descriptor_code(frame.body)
      when Descriptor::TRANSFER
        reader = frame.body_reader
        transfer = TransferCodec.read_transfer(reader)
        session(frame.channel).transfer(transfer, reader.peek)
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
      session = Session.new(self, channel, begin_frame)
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
    end

    private def close_socket
      @running = false
      @socket.close
    rescue ex
      @log.debug { "#{ex.inspect} when closing AMQP 1.0 socket" }
    end
  end
end
