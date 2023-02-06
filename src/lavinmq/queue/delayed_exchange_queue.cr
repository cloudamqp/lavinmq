require "./queue"
require "./durable_queue"

module LavinMQ
  #  class DelayedExchangeQueue < Queue
  #    @ready = Queue::ExpirationReadyQueue.new
  #    @internal = true
  #  end
  #
  #  class DurableDelayedExchangeQueue < DurableQueue
  #    @ready = Queue::ExpirationReadyQueue.new
  #    @internal = true
  #  end
end
