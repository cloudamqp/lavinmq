require "./queue"

module LavinMQ
  class DurableQueue < Queue
    def durable?
      true
    end
  end
end
