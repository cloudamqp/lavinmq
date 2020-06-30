require "./queue"
require "./durable_queue"

module AvalancheMQ
  module DelayedExchangeQueuePublishMixin
    @ready : Queue::ReadyQueue = Queue::SortedReadyQueue.new
    @internal = true
  end

  class DelayedExchangeQueue < Queue
    include DelayedExchangeQueuePublishMixin
  end

  class DurableDelayedExchangeQueue < DurableQueue
    include DelayedExchangeQueuePublishMixin
  end
end
