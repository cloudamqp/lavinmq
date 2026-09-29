module LavinMQ
  class Persister
    # Set by a spec to hold publish confirms back until the channel is
    # closed, as if the disk sync or the followers' acks were slow.
    class_property held_confirms : ::Channel(Nil)? = nil

    private def drain_pending_acks
      Persister.held_confirms.try &.receive?
      previous_def
    end
  end
end
