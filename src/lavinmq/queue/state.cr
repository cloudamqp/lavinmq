module LavinMQ
  enum QueueState
    Running
    Paused
    Flow
    Closed
    Deleted
    Error

    def to_s
      super.downcase
    end
  end
end
