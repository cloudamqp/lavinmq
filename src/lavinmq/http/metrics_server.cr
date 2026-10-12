require "http/server"
require "./retrying_server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/prometheus"

module LavinMQ
  module HTTP
    # Serves the Prometheus metrics endpoints. One instance lives for the whole
    # process: what it reports follows the node's role, so it never has to be
    # closed and rebound when a follower is promoted to leader. It reports as a
    # follower until a server is set with #leader=.
    class MetricsServer
      Log = LavinMQ::Log.for "metrics.server"

      # Passes requests to the leader's controller once there is a leader,
      # otherwise to the follower's.
      private class RoleHandler
        include ::HTTP::Handler

        property leader : PrometheusController?
        property follower : FollowerPrometheusController

        def initialize(@follower)
        end

        def call(context)
          if leader = @leader
            leader.call(context)
          else
            @follower.call(context)
          end
        end
      end

      def initialize(amqp_server : LavinMQ::Server? = nil, clustering_client : LavinMQ::Clustering::Client? = nil)
        @closed = false
        @role = role = RoleHandler.new(FollowerPrometheusController.new(clustering_client))
        handlers = [
          ApiErrorHandler.new,
          ApiDefaultsHandler.new,
          role,
        ] of ::HTTP::Handler
        handlers.unshift(::HTTP::LogHandler.new(log: Log)) if Log.level == ::Log::Severity::Debug
        @http = RetryingServer.new(handlers)
        self.leader = amqp_server if amqp_server
      end

      # Report as leader, with the full set of metrics from the server
      def leader=(server : LavinMQ::Server) : Nil
        @role.leader = PrometheusController.new(server, require_authentication: false)
      end

      # Report as a follower of the leader that the client replicates from
      def follower=(client : LavinMQ::Clustering::Client?) : Nil
        @role.follower = FollowerPrometheusController.new(client)
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
