require "../../server"

module LavinMQ
  # Acts as a proxy between websocket clients and the normal TCP servers
  class WebsocketProxy
    enum Protocol
      MQTT
      AMQP
    end

    def self.new(server : Server)
      ::HTTP::WebSocketHandler.new do |ws, ctx|
        req = ctx.request
        protocol, header_value = pick_protocol(req)

        # Respond with the header value we used to decide protocol
        if value = header_value
          ctx.response.headers["Sec-WebSocket-Protocol"] = value
        end

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

    # Returns Tuple(Protocol, String?) where the string value is the header value
    # used if a header was used to decide protocol
    # It accepts any Sec-WebSocket-Protocol starting with amqp or mqtt and fallbacks
    # to request path then to AMQP.
    private def self.pick_protocol(request : ::HTTP::Request) : {Protocol, String?}
      if protocols = request.headers.get?("Sec-WebSocket-Protocol")
        protocols.each do |protocol|
          case value = protocol
          # "amqp" is registered as amqp 1.0, but we accept any amqp value
          # see https://www.iana.org/assignments/websocket/websocket.xml#subprotocol-name
          when /^amqp/i then return {Protocol::AMQP, value}
            # "mqtt" is registered as mqtt 5.0
            # see https://www.iana.org/assignments/websocket/websocket.xml#subprotocol-name
          when /^mqtt/i then return {Protocol::MQTT, value}
          end
        end
      end

      # Fallback to use path
      case request.path
      when "/mqtt", "/ws/mqtt"
        return {Protocol::MQTT, nil}
      end
      # Default to AMQP
      return {Protocol::AMQP, nil}
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
