require "./client"
require "../stdlib_fixes"

module AvalancheMQ
  class NetworkClient < Client
    getter user, max_frame_size, auth_mechanism, remote_address, heartbeat, channel_max

    @max_frame_size : UInt32
    @channel_max : UInt16
    @heartbeat : UInt16
    @auth_mechanism : String
    @remote_address : Socket::IPAddress
    @local_address : Socket::IPAddress
    @socket : TCPSocket | OpenSSL::SSL::Socket

    def initialize(tcp_socket : TCPSocket,
                   ssl_client : OpenSSL::SSL::Socket?,
                   vhost : VHost,
                   user : User,
                   tune_ok,
                   start_ok)
      @socket = ssl_client || tcp_socket
      @remote_address = tcp_socket.remote_address
      @local_address = tcp_socket.local_address
      log = vhost.log.dup
      log.progname += " client=#{@remote_address}"
      @max_frame_size = tune_ok.frame_max
      @channel_max = tune_ok.channel_max
      @heartbeat = tune_ok.heartbeat
      @auth_mechanism = start_ok.mechanism
      name = "#{@remote_address} -> #{@local_address}"
      super(name, vhost, user, log, start_ok.client_properties)
      @log.info "Connected #{ssl_client.try &.tls_version} #{ssl_client.try &.cipher}"
      spawn heartbeat_loop, name: "Client#heartbeat_loop #{@remote_address}"
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    def self.start(tcp_socket, ssl_client, vhosts, users, log)
      socket = ssl_client || tcp_socket
      remote_address = tcp_socket.remote_address
      proto = uninitialized UInt8[8]
      socket.read_fully(proto.to_slice)
      if proto != AMQP::PROTOCOL_START_0_9_1 && proto != AMQP::PROTOCOL_START_0_9
        socket.write AMQP::PROTOCOL_START_0_9_1.to_slice
        socket.flush
        socket.close
        log.debug { "Unknown protocol #{proto}, closing socket" }
        return
      end

      start = AMQP::Frame::Connection::Start.new
      socket.write_bytes start, ::IO::ByteFormat::NetworkEndian
      socket.flush
      start_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::StartOk) }

      username = password = ""
      case start_ok.mechanism
      when "PLAIN"
        resp = start_ok.response
        i = resp.index('\u0000', 1).not_nil!
        username = resp[1...i]
        password = resp[(i + 1)..-1]
      when "AMQPLAIN"
        io = ::IO::Memory.new(start_ok.response)
        tbl = AMQP::Table.from_io(io, ::IO::ByteFormat::NetworkEndian)
        username = tbl["LOGIN"].as(String)
        password = tbl["PASSWORD"].as(String)
      else "Unsupported authentication mechanism: #{start_ok.mechanism}"
      end

      user = users[username]?
      unless user && user.password == password
        log.warn "Access denied for #{remote_address} using username \"#{username}\""
        props = start_ok.client_properties
        capabilities = props["capabilities"]?.try &.as(Hash(String, AMQP::Field))
        if capabilities && capabilities["authentication_failure_close"]?.try &.as(Bool)
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED",
            start_ok.class_id,
            start_ok.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket, log)
        else
          socket.close
        end
        return
      end
      socket.write_bytes AMQP::Frame::Connection::Tune.new(channel_max: 0_u16,
        frame_max: 131072_u32,
        heartbeat: Config.instance.heartbeat), IO::ByteFormat::NetworkEndian
      socket.flush
      tune_ok = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::TuneOk) }
      open = AMQP::Frame.from_io(socket) { |f| f.as(AMQP::Frame::Connection::Open) }
      if vhost = vhosts[open.vhost]? || nil
        if user.permissions[open.vhost]? || nil
          socket.write_bytes AMQP::Frame::Connection::OpenOk.new, IO::ByteFormat::NetworkEndian
          socket.flush
          return self.new(tcp_socket, ssl_client, vhost, user, tune_ok, start_ok)
        else
          log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
          reply_text = "NOT_ALLOWED - '#{username}' doesn't have access to '#{vhost.name}'"
          socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, reply_text,
            open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
          socket.flush
          close_on_ok(socket, log)
        end
      else
        log.warn "Access denied for #{remote_address} to vhost \"#{open.vhost}\""
        socket.write_bytes AMQP::Frame::Connection::Close.new(530_u16, "NOT_ALLOWED - vhost not found",
          open.class_id, open.method_id), IO::ByteFormat::NetworkEndian
        socket.flush
        close_on_ok(socket, log)
      end
      nil
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error | AMQP::Error::FrameDecode
      log.warn "#{(ex.cause || ex).inspect} while #{remote_address} tried to establish connection"
      nil
    rescue ex : Exception
      log.error "Error while #{remote_address} tried to establish connection #{ex.inspect_with_backtrace}"
      socket.try &.close unless socket.try &.closed?
      nil
    end

    def channel_name_prefix
      @remote_address.to_s
    end

    private def cleanup
      begin
        @socket.close unless @socket.closed?
      rescue ex : IO::Error | Errno | OpenSSL::SSL::Error
      end
      super
    end

    def details_tuple
      {
        channels:          @channels.size,
        connected_at:      @connected_at,
        type:              "network",
        channel_max:       @channel_max,
        timeout:           @heartbeat,
        client_properties: @client_properties,
        vhost:             @vhost.name,
        user:              @user.name,
        protocol:          "AMQP 0-9-1",
        auth_mechanism:    @auth_mechanism,
        host:              @local_address.address,
        port:              @local_address.port,
        peer_host:         @remote_address.address,
        peer_port:         @remote_address.port,
        name:              @name,
        ssl:               @socket.is_a?(OpenSSL::SSL::Socket),
        state:             state,
      }.merge(stats_details)
    end

    private def read_loop
      i = 0
      loop do
        AMQP::Frame.from_io(@socket) do |frame|
          @log.debug { "Read #{frame.inspect}" }
          if (!@running && !frame.is_a?(AMQP::Frame::Connection::Close | AMQP::Frame::Connection::CloseOk))
            @log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
            if frame.is_a?(AMQP::Frame::Body)
              @log.debug "Skipping body"
              frame.body.skip(frame.body_size)
            end
            next true
          end
          process_frame(frame)
        end || break
        Fiber.yield if (i += 1) % 1000 == 0
      end
    rescue ex : AMQP::Error::NotImplemented
      @log.error { "#{ex} when reading from socket" }
      if ex.channel > 0
        close_channel(ex, 540_u16, "Not implemented")
      else
        close_connection(ex, 540_u16, "Not implemented")
      end
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error | AMQP::Error::FrameDecode | ::Channel::ClosedError
      @log.info { "Lost connection, while reading (#{ex.inspect})" } unless closed?
      cleanup
    rescue ex : Exception
      @log.error { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
      @running = false
    end

    def send(frame : AMQP::Frame)
      return false if closed?
      @log.debug { "Send #{frame.inspect}" }
      @write_lock.synchronize do
        @socket.write_bytes frame, IO::ByteFormat::NetworkEndian
        @socket.flush
      end
      @send_oct_count += frame.bytesize + 8
      case frame
      when AMQP::Frame::Connection::CloseOk
        @log.info "Disconnected"
        cleanup
        return false
      end
      true
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error
      @log.info { "Lost connection, while sending (#{ex.inspect})" } unless closed?
      cleanup
      false
    rescue ex : IO::Timeout
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        peer_host: @remote_address.address,
        peer_port: @remote_address.port,
        name:      @name,
      }
    end

    @write_lock = Mutex.new

    def deliver(frame, msg)
      @write_lock.synchronize do
        @log.debug { "Send #{frame.inspect}" }
        @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += frame.bytesize + 8
        header = AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
        @log.debug { "Send #{header.inspect}" }
        @socket.write_bytes header, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += header.bytesize + 8
        pos = 0
        while pos < msg.size
          length = Math.min(msg.size - pos, @max_frame_size - 8).to_u32
          @log.debug { "Send BodyFrame (pos #{pos}, length #{length})" }
          body = AMQP::Frame::Body.new(frame.channel, length, msg.body_io)
          @socket.write_bytes body, ::IO::ByteFormat::NetworkEndian
          @send_oct_count += body.bytesize + 8
          pos += length
        end
        @log.debug { "Flushing" }
        @socket.flush
      end
      true
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error
      @log.info { "Lost connection, while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex : IO::Timeout
      @log.info { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      @log.error { "Delivery exception: #{ex.inspect_with_backtrace}" }
      raise ex
    end

    private def heartbeat_loop
      return if @heartbeat == 0
      @log.debug { "Starting heartbeat loop with #{@heartbeat}s interval" }
      loop do
        sleep @heartbeat
        break unless @running
        send(AMQP::Frame::Heartbeat.new) || break
      end
    end
  end
end
