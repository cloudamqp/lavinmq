require "./client"
require "./amqp_connection"

module AvalancheMQ
  class NetworkClient < Client
    Log = ::Log.for(self)

    getter user, max_frame_size, auth_mechanism, remote_address, heartbeat, channel_max

    @max_frame_size : UInt32
    @channel_max : UInt16
    @heartbeat : UInt16
    @auth_mechanism : String

    def initialize(@socket : TCPSocket | OpenSSL::SSL::Socket | UNIXSocket,
                   @remote_address : Socket::IPAddress,
                   @local_address : Socket::IPAddress,
                   vhost : VHost,
                   user : User,
                   tune_ok,
                   start_ok)
      @max_frame_size = tune_ok.frame_max
      @channel_max = tune_ok.channel_max
      @heartbeat = tune_ok.heartbeat
      @auth_mechanism = start_ok.mechanism
      name = "#{@remote_address} -> #{@local_address}"
      super(name, vhost, user, start_ok.client_properties)
      spawn read_loop, name: "Client#read_loop #{@remote_address}"
    end

    def self.start(socket, remote_address, local_address, vhosts, users)
      AMQPConnection.start(socket, remote_address, local_address, vhosts, users)
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
      i = 0
      loop do
        AMQP::Frame.from_io(@socket) do |frame|
          if (i += 1) == 8192
            i = 0
            Fiber.yield
          end
          if @running
            process_frame(frame)
          else
            case frame
            when AMQP::Frame::Connection::Close, AMQP::Frame::Connection::CloseOk
              process_frame(frame)
            when AMQP::Frame::Body
              Log.debug { "Skipping body, waiting for Close(Ok)" }
              frame.body.skip(frame.body_size)
              true
            else
              Log.debug { "Discarding #{frame.class.name}, waiting for Close(Ok)" }
              true
            end
          end
        end || break
      rescue IO::TimeoutError
        send_heartbeat || break
      end
    rescue ex : IO::Error | OpenSSL::SSL::Error | AMQP::Error::FrameDecode | ::Channel::ClosedError
      Log.debug(exception: ex) { "Lost connection, while reading (#{ex.inspect})" } unless closed?
      cleanup
    rescue ex : Exception
      Log.error(exception: ex) { "Unexpected error, while reading: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    ensure
      @running = false
    end

    private def send_heartbeat
      if @last_heartbeat + @heartbeat.seconds < RoughTime.utc
        send(AMQP::Frame::Heartbeat.new)
      else
        true
      end
    end

    def send(frame : AMQP::Frame)
      return false if closed?
      #Log.debug { "Send #{frame.inspect}" }
      @write_lock.synchronize do
        @socket.write_bytes frame, IO::ByteFormat::NetworkEndian
        @socket.flush
      end
      @send_oct_count += 8_u64 + frame.bytesize
      case frame
      when AMQP::Frame::Connection::CloseOk
        Log.debug { "Disconnected" }
        cleanup
        false
      else
        true
      end
    rescue ex : IO::Error | OpenSSL::SSL::Error
      Log.debug(exception: ex) { "Lost connection, while sending (#{ex.inspect})" } unless closed?
      cleanup
      false
    rescue ex : IO::TimeoutError
      Log.info(exception: ex) { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      Log.error(exception: ex) { "Unexpected error, while sending: #{ex.inspect_with_backtrace}" }
      send AMQP::Frame::Connection::Close.new(541_u16, "Internal error", 0_u16, 0_u16)
    end

    def connection_details
      {
        peer_host: @remote_address.address,
        peer_port: @remote_address.port,
        name:      @name,
      }
    end

    @write_lock = Mutex.new(:unchecked)

    def deliver(frame, msg)
      @write_lock.synchronize do
        #Log.debug { "Send #{frame.inspect}" }
        @socket.write_bytes frame, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += 8_u64 + frame.bytesize
        header = AMQP::Frame::Header.new(frame.channel, 60_u16, 0_u16, msg.size, msg.properties)
        #Log.debug { "Send #{header.inspect}" }
        @socket.write_bytes header, ::IO::ByteFormat::NetworkEndian
        @send_oct_count += 8_u64 + header.bytesize
        pos = 0
        while pos < msg.size
          length = Math.min(msg.size - pos, @max_frame_size - 8).to_u32
          #Log.debug { "Send BodyFrame (pos #{pos}, length #{length})" }
          body = AMQP::Frame::Body.new(frame.channel, length, msg.body_io)
          @socket.write_bytes body, ::IO::ByteFormat::NetworkEndian
          @send_oct_count += 8_u64 + body.bytesize
          pos += length
        end
        #Log.debug { "Flushing" }
        @socket.flush
      end
      true
    rescue ex : IO::Error | OpenSSL::SSL::Error | AMQ::Protocol::Error::FrameEncode
      Log.debug(exception: ex) { "Lost connection, while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex : IO::TimeoutError
      Log.info(exception: ex) { "Timeout while sending (#{ex.inspect})" }
      cleanup
      false
    rescue ex
      Log.error(exception: ex) { "Delivery exception: #{ex.inspect_with_backtrace}" }
      raise ex
    end

    def cleanup
      super
      begin
        @socket.close unless @socket.closed?
      rescue ex
        Log.debug(exception: ex) { "error when closing socket: #{ex.inspect_with_backtrace}" }
      end
    end
  end
end
