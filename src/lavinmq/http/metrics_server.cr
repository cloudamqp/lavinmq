require "http/server"
require "../../stdlib/http_server"
require "json"
require "./constants"
require "./listener"
require "./handler/*"
require "./controller"
require "./controller/prometheus"

module LavinMQ
  module HTTP
    # Serves Prometheus metrics for the whole life of the process. It reports
    # the broker's metrics once this node serves (see #amqp_server=), and
    # *metrics*, the runner's own (see Runner#write_metrics), throughout.
    class MetricsServer
      Log = LavinMQ::Log.for "metrics.server"

      # Hands requests to the broker's controller once there is one
      private class Source
        include ::HTTP::Handler
        property leader : PrometheusController?

        def initialize(@follower : FollowerPrometheusController)
        end

        def call(context)
          (@leader || @follower).call(context)
        end
      end

      def initialize(amqp_server : LavinMQ::Server? = nil, @metrics : Proc(PrometheusWriter, Nil)? = nil)
        @closed = false
        @source = Source.new(FollowerPrometheusController.new(@metrics))
        handlers = [
          ApiErrorHandler.new,
          ApiDefaultsHandler.new,
          @source,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new(log: Log)) if Log.level == ::Log::Severity::Debug
        @http = ::HTTP::Server.new(handlers)
        amqp_server.try { |s| self.amqp_server = s }
      end

      # Reports the broker's metrics from now on
      def amqp_server=(server : LavinMQ::Server) : Nil
        @source.leader = PrometheusController.new(server, require_authentication: false, metrics: @metrics)
      end

      def bind_tcp(address, port)
        tcp = HTTP.tcp_listener(address, port) { |socket| @http.bind(socket) }
        Log.info { "Bound to #{tcp.local_address}" }
        tcp.local_address
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
