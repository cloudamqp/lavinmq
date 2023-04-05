require "../../server"

module LavinMQ
  class AMQPWebsocket
    def self.new(amqp_server : Server)
      ::HTTP::WebSocketHandler.new do |ws, ctx|
        req = ctx.request
        local_address = req.local_address.as(Socket::IPAddress)
        remote_address = req.remote_address.as(Socket::IPAddress)
        connection_info = ConnectionInfo.new(remote_address, local_address)
        io = WebSocketIO.new(ws)
        spawn amqp_server.handle_connection(io, connection_info), name: "HandleWSconnection #{remote_address}"
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

    def read_timeout=(timeout)
      @r.read_timeout = timeout
    end

    def fd
      io = @ws.@ws.@io
      return io.fd if io.responds_to?(:fd)
      0
    end
  end
end
