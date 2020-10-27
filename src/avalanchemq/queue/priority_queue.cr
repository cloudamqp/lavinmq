require "./queue"
require "./durable_queue"

module AvalancheMQ
  module PriorityQueueMixin
    @ready : Queue::ReadyQueue = Queue::PriorityReadyQueue.new
  end

  class PriorityQueue < Queue
    include PriorityQueueMixin
  end

  class DurablePriorityQueue < DurableQueue
    include PriorityQueueMixin
  end
end
