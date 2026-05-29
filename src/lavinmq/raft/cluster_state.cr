module LavinMQ::Raft
  # Immutable cluster state snapshot returned by ClusterStateMachine#state.
  # The internal `secret` and `isr` are SHARED references with the state
  # machine — treat as frozen. Apply replaces (never mutates in place), so
  # holding a previously-returned snapshot is safe.
  struct ClusterState
    getter secret : String
    getter isr : Set(Int32)

    def initialize(@secret : String, @isr : Set(Int32))
    end

    EMPTY = ClusterState.new("", Set(Int32).new)
  end
end
