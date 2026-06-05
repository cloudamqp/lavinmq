require "http/server"
require "json"
require "raft/http/handler"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/prometheus"
require "./raft_handler_wrapper"
require "../raft/cluster_command"
require "../raft/runner"

module LavinMQ
  module HTTP
    class MetricsServer
      Log = LavinMQ::Log.for "metrics.server"

      def initialize(amqp_server : LavinMQ::Server? = nil, raft_runner : LavinMQ::Raft::Runner? = nil)
        @closed = false
        controller = if s = amqp_server
                       PrometheusController.new(s, require_authentication: false, raft_runner: raft_runner)
                     else
                       FollowerPrometheusController.new(raft_runner: raft_runner)
                     end
        handlers = [
          ApiErrorHandler.new,
          ApiDefaultsHandler.new,
          controller,
        ] of ::HTTP::Handler
        if rr = raft_runner
          # Mounting raft.cr's handler here gives every node (leader and
          # follower) a scrape-able /raft/metrics, /raft/status, etc. The
          # main HTTP server still exposes the same routes on the leader
          # behind auth. NOTE: the metrics server has no auth, so
          # /raft/admin/* is reachable without credentials on this port —
          # acceptable for a localhost-bound metrics port; lock down if
          # exposing externally.
          raft_handler = ::Raft::HTTP::Handler(LavinMQ::Raft::ClusterCommand).new(
            rr.server.node,
            rr.transport,
            rr.advertised_address,
          )
          handlers << RaftHandlerWrapper.new(raft_handler)
        end
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
