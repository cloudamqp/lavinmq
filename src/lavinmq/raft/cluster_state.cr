module LavinMQ::Raft
  # An immutable snapshot of cluster state, published by ClusterStateMachine
  # after each apply for cross-thread-safe reading via an Atomic reference.
  #
  # Must be a class (not a struct) so that Atomic(ClusterState) is supported.
  # Treat as frozen: do not mutate `isr` or `secret` from outside the state
  # machine — they back published snapshots. (Crystal does not enforce this.)
  class ClusterState
    getter secret : String
    getter isr : Set(UInt64)

    def initialize(@secret : String, @isr : Set(UInt64))
    end

    EMPTY = ClusterState.new("", Set(UInt64).new)
  end
end
