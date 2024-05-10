module LavinMQ
  enum QueueState
    Running
    Paused
    Flow
    Closed
    Deleted

    def to_s
      super.downcase
    end
  end
end
