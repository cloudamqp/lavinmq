require "./client"
require "./amqp_connection"

module AvalancheMQ
  class NetworkClient < Client
    getter user, max_frame_size, auth_mechanism, remote_address, heartbeat, channel_max

    @max_frame_size : UInt32
    @channel_max : UInt16
    @heartbeat : UInt16
    @auth_mechanism : String
    @remote_address : Socket::IPAddress
    @local_address : Socket::IPAddress
    @socket : Socket | OpenSSL::SSL::Socket
    @heartbeat_loop : Fiber

    def initialize(@socket,
                   @remote_address,
                   @local_address,
                   vhost : VHost,
                   user : User,
                   tune_ok,
                   start_ok)
      log = vhost.log.dup
      log.progname += " client=#{@remote_address}"
      @max_frame_size = tune_ok.frame_max
      @channel_max = tune_ok.channel_max
      @heartbeat = tune_ok.heartbeat
      @auth_mechanism = start_ok.mechanism
      name = "#{@remote_address} -> #{@local_address}"
      super(name, vhost, user, log, start_ok.client_properties)
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
      @heartbeat_loop = spawn(heartbeat_loop, name: "Client#heartbeat_loop #{@remote_address}")
    end

    def self.start(socket, remote_address, local_address, vhosts, users, log)
      AMQPConnection.start(socket, remote_address, local_address, vhosts, users, log)
    end

    def channel_name_prefix
      @remote_address.to_s
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
        @log.debug "Disconnected"
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
    rescue ex : IO::Error | Errno | OpenSSL::SSL::Error | AMQ::Protocol::Error::FrameEncode
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

    def cleanup
      super
      #@heartbeat_loop.wakeup unless @heartbeat_loop.dead?
      begin
        @socket.close unless @socket.closed?
      rescue ex : IO::Error | Errno | OpenSSL::SSL::Error
      end
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
