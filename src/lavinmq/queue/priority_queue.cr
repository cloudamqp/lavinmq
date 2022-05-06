require "./queue"
require "./durable_queue"

module LavinMQ
  class PriorityQueue < Queue
    @ready = Queue::PriorityReadyQueue.new
  end

  class DurablePriorityQueue < DurableQueue
    @ready = Queue::PriorityReadyQueue.new
  end
end
