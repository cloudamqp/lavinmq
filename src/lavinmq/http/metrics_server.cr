require "http/server"
require "json"
require "./constants"
require "./handler/*"
require "./controller"
require "./controller/prometheus"
require "../raft/cluster_command"
require "../raft/elector"

module LavinMQ
  module HTTP
    class MetricsServer
      Log = LavinMQ::Log.for "metrics.server"

      def initialize(amqp_server : LavinMQ::Server? = nil, raft_elector : LavinMQ::Raft::Elector? = nil)
        @closed = false
        controller = if s = amqp_server
                       PrometheusController.new(s, require_authentication: false, raft_elector: raft_elector)
                     else
                       FollowerPrometheusController.new(raft_elector: raft_elector)
                     end
        handlers = [
          ApiErrorHandler.new,
          ApiDefaultsHandler.new,
          controller,
        ] of ::HTTP::Handler
        if rr = raft_elector
          # Read-only surface only: every node (leader and follower) gets a
          # scrape-able /raft/metrics, /raft/status, etc. The mutating
          # /raft/admin/* routes are mounted solely on the main HTTP server,
          # behind authentication — this port has none.
          handlers << rr.status_handler
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
