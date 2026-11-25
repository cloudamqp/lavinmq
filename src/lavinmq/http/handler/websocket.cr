require "http/server/handler"
require "http/web_socket"
require "../../connection_info"
require "../../config"
require "../../../stdlib/socket_shutdown"

module LavinMQ
  class WebSocketHandler
    include ::HTTP::Handler

    enum Protocol
      MQTT
      AMQP
    end

    SUPPORTED_PROTOCOLS = {
      /^amqp/i => Protocol::AMQP,
      /^mqtt/i => Protocol::MQTT,
    }

    def initialize(&@proc : WebSocketIO, ::HTTP::Server::Context, Protocol? ->)
    end

    def call(context) : Nil
      unless websocket_upgrade_request? context.request
        return call_next context
      end

      response = context.response

      version = context.request.headers["Sec-WebSocket-Version"]?
      unless version == ::HTTP::WebSocket::Protocol::VERSION
        response.status = :upgrade_required
        response.headers["Sec-WebSocket-Version"] = ::HTTP::WebSocket::Protocol::VERSION
        return
      end

      key = context.request.headers["Sec-WebSocket-Key"]?

      unless key
        response.respond_with_status(:bad_request)
        return
      end

      accept_code = ::HTTP::WebSocket::Protocol.key_challenge(key)

      response.status = :switching_protocols
      response.headers["Upgrade"] = "websocket"
      response.headers["Connection"] = "Upgrade"
      response.headers["Sec-WebSocket-Accept"] = accept_code

      protocol = pick_sub_protocol(context)

      response.upgrade do |io|
        # Writes, and closing the connection, wait for the client's write
        # lock, so a client that stops reading mustn't block a write forever
        if io.responds_to?(:write_timeout=)
          io.write_timeout = LavinMQ::Config.instance.tcp_send_timeout.seconds
        end
        @proc.call(WebSocketIO.new(io), context, protocol)
      end
    end

    private def pick_sub_protocol(context) : Protocol?
      return unless protocols = context.request.headers["Sec-WebSocket-Protocol"]?
      protocols.split(",", remove_empty: true) do |protocol|
        protocol = protocol.strip
        SUPPORTED_PROTOCOLS.each do |pattern, value|
          if pattern.matches?(protocol)
            context.response.headers["Sec-WebSocket-Protocol"] = protocol
            return value
          end
        end
      end
    end

    private def websocket_upgrade_request?(request)
      return false unless upgrade = request.headers["Upgrade"]?
      return false unless upgrade.compare("websocket", case_insensitive: true) == 0

      request.headers.includes_word?("Connection", "Upgrade")
    end
  end

  # Acts as a proxy between websocket clients and the normal TCP servers
  class WebsocketProxy
    def self.new(amqp_server : LavinMQ::AMQP::Server, mqtt_server : LavinMQ::MQTT::Server)
      WebSocketHandler.new do |io, ctx, protocol|
        req = ctx.request
        protocol ||= fallback_protocol(req)

        local_address = req.local_address.as?(Socket::IPAddress) ||
                        Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        remote_address = req.remote_address.as?(Socket::IPAddress) ||
                         Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        connection_info = ConnectionInfo.new(remote_address, local_address)

        case protocol
        in .mqtt?
          Log.debug { "Protocol: mqtt" }
          Fiber.current.name = "HandleWSconnection MQTT #{remote_address}"
          mqtt_server.handle_connection(io, connection_info)
        in .amqp?
          Log.debug { "Protocol: amqp" }
          Fiber.current.name = "HandleWSconnection AMQP #{remote_address}"
          amqp_server.handle_connection(io, connection_info)
        end
      end
    end

    private def self.fallback_protocol(request : ::HTTP::Request) : WebSocketHandler::Protocol
      case request.path
      when "/mqtt", "/ws/mqtt"
        return WebSocketHandler::Protocol::MQTT
      end
      WebSocketHandler::Protocol::AMQP
    end
  end

  # Exposes a WebSocket as a byte stream to the AMQP and MQTT servers. Frame
  # payloads are read straight into the caller's buffer, so a fragmented
  # message needs no reassembly. Every frame the server sends, data, pong or
  # close, goes through one write lock, like a client's write lock.
  class WebSocketIO < IO
    include IO::Buffered

    CLOSE_FRAME_TIMEOUT = 1.second

    private enum WriteState
      Idle
      Writing
      Closed
    end

    @write_lock = Mutex.new
    @write_state = Atomic(WriteState).new(WriteState::Idle)
    @eof = false
    @read_timeout : Time::Span?
    @data_deadline : Time::Instant?

    # *socket* is the connection the WebSocket runs over
    def initialize(@socket : IO)
      @ws = ::HTTP::WebSocket::Protocol.new(@socket, masked: false, sync_close: false)
      self.buffer_size = 4096
    end

    def unbuffered_read(slice : Bytes) : Int32
      return 0 if @eof
      loop do
        info = @ws.receive(slice)
        case info.opcode
        in .binary?, .continuation?
          next if info.size.zero?
          refresh_data_deadline
          return info.size
        in .ping?
          check_data_deadline
          write_frame(slice[0, info.size], :pong) if info.final
        in .pong?
          # An unsolicited pong isn't answered
          check_data_deadline
        in .close?
          @eof = true
          code = ::HTTP::WebSocket::CloseCode.new(IO::ByteFormat::NetworkEndian.decode(UInt16, slice)) if info.size >= 2
          send_close_frame(code || ::HTTP::WebSocket::CloseCode::NormalClosure)
          return 0
        in .text?
          @eof = true
          send_close_frame(:unsupported_data)
          return 0
        end
      end
    rescue ex : IO::Error | OpenSSL::SSL::Error
      raise ex
    rescue ex
      # A malformed frame is a broken connection to the protocol handler,
      # not an unexpected exception
      raise IO::Error.new("WebSocket #{ex.message}", cause: ex)
    end

    def unbuffered_write(slice : Bytes) : Nil
      write_frame(slice, :binary)
    end

    def unbuffered_flush
    end

    def unbuffered_rewind
    end

    def unbuffered_close
      return if @closed
      @closed = true
      send_close_frame
    end

    # Pings and pongs don't count as activity, the timeout is for data, like
    # the MQTT keepalive. The socket's own timeout covers a silent connection.
    def read_timeout=(timeout : Time::Span?)
      @read_timeout = timeout
      refresh_data_deadline
      socket = @socket
      socket.read_timeout = timeout if socket.responds_to?(:read_timeout=)
    end

    private def refresh_data_deadline : Nil
      @data_deadline = @read_timeout.try { |timeout| Time.instant + timeout }
    end

    private def check_data_deadline : Nil
      if (deadline = @data_deadline) && Time.instant > deadline
        raise IO::TimeoutError.new("Read timed out")
      end
    end

    # Shuts down the underlying connection, see `Socket#shutdown_read_write`.
    # A close frame is sent first, unless a frame is being written: the
    # shutdown mustn't wait for a write to a client that stopped reading, and
    # a close frame in the middle of another frame would corrupt it.
    def shutdown_read_write : Nil
      socket = @socket
      # Don't let a client that stopped reading hold up the close for tcp_send_timeout
      socket.write_timeout = CLOSE_FRAME_TIMEOUT if socket.responds_to?(:write_timeout=)
      _, ok = @write_state.compare_and_set(WriteState::Idle, WriteState::Closed)
      write_close_frame if ok
      socket.shutdown_read_write if socket.responds_to?(:shutdown_read_write)
    end

    private def write_frame(payload : Bytes, opcode : ::HTTP::WebSocket::Protocol::Opcode) : Nil
      @write_lock.synchronize do
        _, ok = @write_state.compare_and_set(WriteState::Idle, WriteState::Writing)
        raise IO::Error.new("Closed stream") unless ok
        begin
          @ws.send(payload, opcode)
          @write_state.compare_and_set(WriteState::Writing, WriteState::Idle)
        rescue ex
          # Part of the frame may have been written, so nothing can follow it
          @write_state.set(WriteState::Closed)
          raise ex
        end
      end
    end

    # Sent once, after the frame being written, if any. A client that sent a
    # close frame still reads, so that write completes. Nothing is written
    # after the close frame.
    private def send_close_frame(code : ::HTTP::WebSocket::CloseCode = :normal_closure) : Nil
      @write_lock.synchronize do
        _, ok = @write_state.compare_and_set(WriteState::Idle, WriteState::Closed)
        write_close_frame(code) if ok
      end
    end

    private def write_close_frame(code : ::HTTP::WebSocket::CloseCode = :normal_closure) : Nil
      @ws.close(code)
    rescue IO::Error | OpenSSL::SSL::Error
      # The connection is already gone, so the close frame can't be sent
    end
  end
end
