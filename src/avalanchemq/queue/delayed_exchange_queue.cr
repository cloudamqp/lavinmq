require "./queue"
require "./durable_queue"

module AvalancheMQ
  class DelayedExchangeQueue < Queue
    @ready = Queue::ExpirationReadyQueue.new
    @internal = true
  end

  class DurableDelayedExchangeQueue < DurableQueue
    @ready = Queue::ExpirationReadyQueue.new
    @internal = true
  end
end
