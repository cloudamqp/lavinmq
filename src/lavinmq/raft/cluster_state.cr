module LavinMQ::Raft
  # Immutable cluster state snapshot returned by ClusterStateMachine#state.
  # The internal `isr` is a SHARED reference with the state machine — treat as
  # frozen. Apply replaces it (never mutates in place), so holding a
  # previously-returned snapshot is safe.
  struct ClusterState
    getter isr : Set(Int32)

    def initialize(@isr : Set(Int32))
    end

    EMPTY = ClusterState.new(Set(Int32).new)
  end
end
