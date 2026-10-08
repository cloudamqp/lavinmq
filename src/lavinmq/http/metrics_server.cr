require "http/server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/prometheus"

module LavinMQ
  module HTTP
    # Serves Prometheus metrics for the whole life of the process. What it
    # reports follows the node's role: a follower's replication client while
    # following, and the broker once it's serving (see #amqp_server=).
    class MetricsServer
      Log = LavinMQ::Log.for "metrics.server"

      # Hands requests to the broker's controller once there is one
      private class Source
        include ::HTTP::Handler
        property leader : PrometheusController?
        getter follower : FollowerPrometheusController

        def initialize(@follower)
        end

        def call(context)
          (@leader || @follower).call(context)
        end
      end

      def initialize(amqp_server : LavinMQ::Server? = nil, clustering_client : LavinMQ::Clustering::Client? = nil)
        @closed = false
        @source = Source.new(FollowerPrometheusController.new(clustering_client))
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
        @source.leader = PrometheusController.new(server, require_authentication: false)
      end

      # The replication client to report on while following, nil when not
      def clustering_client=(client : LavinMQ::Clustering::Client?) : Nil
        @source.follower.clustering_client = client
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
