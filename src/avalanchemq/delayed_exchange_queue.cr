require "./queue"
require "./durable_queue"

module AvalancheMQ
  module DelayedExchangeQueueMixin
    @ready : Queue::ReadyQueue = Queue::SortedReadyQueue.new
    @internal = true
  end

  class DelayedExchangeQueue < Queue
    include DelayedExchangeQueueMixin
  end

  class DurableDelayedExchangeQueue < DurableQueue
    include DelayedExchangeQueueMixin
  end
end
