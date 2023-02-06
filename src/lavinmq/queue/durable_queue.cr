require "./queue"

module LavinMQ
  class DurableQueue < Queue
    @durable = true
  end
end
