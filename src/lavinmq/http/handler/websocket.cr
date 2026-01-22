require "../../server"

module LavinMQ
  class WebSocketHandler
    include ::HTTP::Handler

    enum Protocol
      MQTT
      AMQP
    end

    SUPPORTED_PROTOCOLS = {
      /^amqp/ => Protocol::AMQP,
      /^mqtt/ => Protocol::MQTT,
    }

    def initialize(&@proc : ::HTTP::WebSocket, ::HTTP::Server::Context, Protocol? ->)
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
        ws_session = ::HTTP::WebSocket.new(io, sync_close: false)
        @proc.call(ws_session, context, protocol)
        ws_session.run
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
    def self.new(server : LavinMQ::Server)
      WebSocketHandler.new do |ws, ctx, protocol|
        req = ctx.request
        protocol ||= pick_protocol(req)

        local_address = req.local_address.as?(Socket::IPAddress) ||
                        Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        remote_address = req.remote_address.as?(Socket::IPAddress) ||
                         Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        connection_info = ConnectionInfo.new(remote_address, local_address)
        io = WebSocketIO.new(ws)

        case protocol
        in .mqtt?
          Log.debug { "Protocol: mqtt" }
          spawn server.handle_connection(io, connection_info, Server::Protocol::MQTT), name: "HandleWSconnection MQTT #{remote_address}"
        in .amqp?
          Log.debug { "Protocol: amqp" }
          spawn server.handle_connection(io, connection_info, Server::Protocol::AMQP), name: "HandleWSconnection AMQP #{remote_address}"
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

  class WebSocketIO < IO
    include IO::Buffered

    def initialize(@ws : ::HTTP::WebSocket)
      @r, @w = IO.pipe
      @r.read_buffering = false
      @w.sync = true
      @ws.on_binary do |slice|
        @w.write(slice)
      end
      @ws.on_close do |_code, _message|
        close
      end
      self.buffer_size = 4096
    end

    def unbuffered_read(slice : Bytes)
      @r.read(slice)
    end

    def unbuffered_write(slice : Bytes) : Nil
      @ws.send(slice)
    end

    def unbuffered_flush
    end

    def unbuffered_rewind
    end

    def unbuffered_close
      return if @closed
      @closed = true
      @r.close
      @w.close
      @ws.close
    end

    def read_timeout=(timeout : Time::Span?)
      @r.read_timeout = timeout
    end
  end
end
