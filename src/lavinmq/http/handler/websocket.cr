require "../../server"

module LavinMQ
  # Acts as a proxy between websocket clients and the normal TCP servers
  class WebsocketProxy
    def self.new(server : Server)
      ::HTTP::WebSocketHandler.new do |ws, ctx|
        req = ctx.request
        local_address = req.local_address.as?(Socket::IPAddress) ||
                        Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        remote_address = req.remote_address.as?(Socket::IPAddress) ||
                         Socket::IPAddress.new("127.0.0.1", 0) # Fake when UNIXAddress
        connection_info = ConnectionInfo.new(remote_address, local_address)
        io = WebSocketIO.new(ws)
        case req.path
        when "/mqtt", "/ws/mqtt"
          spawn server.handle_connection(io, connection_info, Server::Protocol::MQTT), name: "HandleWSconnection MQTT #{remote_address}"
        else
          spawn server.handle_connection(io, connection_info, Server::Protocol::AMQP), name: "HandleWSconnection AMQP #{remote_address}"
        end
      end
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
        self.close
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

    # TODO: remove when amqp-client is updated
    def read_timeout=(timeout : Number)
      self.read_timeout = timeout.seconds
    end

    def read_timeout=(timeout : Time::Span?)
      @r.read_timeout = timeout
    end
  end
end
