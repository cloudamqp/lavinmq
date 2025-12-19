require "./source"
require "http/server"
require "http/server/handler"

module LavinMQ
  module Shovel
    class HTTPSource < Source
      include HTTP::Handler
      Log = LavinMQ::Log.for "http_source"

      class HTTPServer
        def initialize(controller : ::HTTP::Handler)
          handlers = [
            # ApiErrorHandler.new,
            # ApiDefaultsHandler.new,
            controller,
          ] of ::HTTP::Handler
          handlers.unshift(::HTTP::LogHandler.new(log: Log))
          @http = ::HTTP::Server.new(handlers)
        end

        def start(address, port)
          addr = @http.bind_tcp address, port
          Log.info { "Bound to #{addr}" }
          @http.listen
        end

        def stop
          @http.close
        end
      end

      def initialize(@name : String, @address : String, @port : Int32)
        @ch = Channel(ShovelMessage).new
        @server = HTTPServer.new(self)
      end

      def start
        @server.try(&.start(@address, @port))
      end

      def stop
        @server.try &.stop
      end

      def ack(delivery_tag, batch = true)
      end

      def call(context)
        body = context.request.body
        return unless body
        io = IO::Memory.new
        IO.copy(body, io)
        io.rewind

        msg = ShovelMessage.new("hej")
        @ch.send msg
      end

      def each(&blk : ShovelMessage -> Nil)
        spawn do
          loop do
            msg = @ch.receive
            blk.call(msg)
          end
        end
      end
    end
  end
end
