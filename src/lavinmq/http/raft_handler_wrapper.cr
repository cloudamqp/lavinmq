require "http/server/handler"
require "raft/http/status_handler"
require "raft/http/admin_handler"
require "../raft/cluster_command"

module LavinMQ
  module HTTP
    # Concrete (non-generic) wrapper around raft.cr's generic HTTP handlers
    # (`StatusHandler(T)` / `AdminHandler(T)`) so they can participate in
    # `Array(::HTTP::Handler)` chain dispatch.
    #
    # Crystal's codegen for the virtual dispatch in stdlib
    # `HTTP::Handler#call_next` (`next_handler.call(context)`) builds a
    # type-id case table over concrete classes that `include ::HTTP::Handler`.
    # Generic-class instantiations are not enumerated as branches in that
    # table — so a generic raft handler placed directly in the handler array
    # traps with `brk #1` when the chain tries to dispatch into it.
    #
    # This wrapper is a plain concrete class, so it gets its own branch.
    # Build instances via `Raft::Elector#status_handler` / `#admin_handler`.
    class RaftHandlerWrapper
      include ::HTTP::Handler

      alias Inner = ::Raft::HTTP::StatusHandler(LavinMQ::Raft::ClusterCommand) |
                    ::Raft::HTTP::AdminHandler(LavinMQ::Raft::ClusterCommand)

      def initialize(@inner : Inner)
      end

      def call(context : ::HTTP::Server::Context)
        # The inner handler's call_next consults its own `next` pointer, not
        # ours; forward ours so a fall-through continues down the outer chain
        # (stdlib responds 404 only when there is no next handler at all).
        @inner.next = @next
        @inner.call(context)
      end
    end
  end
end
