require "./queue"
require "./durable_queue"

module AvalancheMQ
  class PriorityQueue < Queue
  end

  class DurablePriorityQueue < DurableQueue
  end
end
