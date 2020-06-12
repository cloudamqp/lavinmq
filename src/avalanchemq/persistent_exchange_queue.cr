require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true
  end
end
