require "http/server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/prometheus"

module LavinMQ
  module HTTP
    class MetricsServer
      Log = LavinMQ::Log.for "metrics.server"

      def initialize(amqp_server : LavinMQ::Server? = nil)
        @closed = false
        controller = if s = amqp_server
                       PrometheusController.new(s)
                     else
                       FollowerPrometheusController.new
                     end
        handlers = [
          ApiErrorHandler.new,
          ApiDefaultsHandler.new,
          controller,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new(log: Log)) if Log.level == ::Log::Severity::Debug
        @http = ::HTTP::Server.new(handlers)
      end

      def bind_tcp(address, port)
        addr = @http.bind_tcp address, port
        Log.info { "Bound to #{addr}" }
        addr
      end

      def listen
        @http.listen
      end

      def close
        return if @closed
        @closed = true
        @http.try &.close
      end
    end
  end
end
