module LavinMQ
  enum QueueState
    Running
    Paused
    Failed # message data can't be read, the queue has to be deleted
    Flow
    Closed
    Deleted

    def to_s
      super.downcase
    end
  end
end
