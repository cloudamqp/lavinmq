require "http/server/handler"
require "raft/http/handler"
require "../raft/cluster_command"

module LavinMQ
  module HTTP
    # Concrete (non-generic) wrapper around raft.cr's generic
    # `Raft::HTTP::Handler(T)` so it can participate in
    # `Array(::HTTP::Handler)` chain dispatch.
    #
    # Crystal's codegen for the virtual dispatch at `handler.cr:30`
    # (`next_handler.call(context)`) builds a type-id case table over
    # concrete classes that `include ::HTTP::Handler`. Generic-class
    # instantiations are not enumerated as branches in that table — so a
    # `Raft::HTTP::Handler(LavinMQ::Raft::ClusterCommand)` placed
    # directly in the handler array traps with `brk #1` when the chain
    # tries to dispatch into it.
    #
    # This wrapper is a plain concrete class, so it gets its own branch.
    # See docs/superpowers/crystal-bug-report.md for the full diagnosis.
    class RaftHandlerWrapper
      include ::HTTP::Handler

      def initialize(@inner : ::Raft::HTTP::Handler(LavinMQ::Raft::ClusterCommand))
      end

      def call(context : ::HTTP::Server::Context)
        @inner.call(context)
      end
    end
  end
end
