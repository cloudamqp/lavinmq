require "./durable_queue"

module AvalancheMQ
  class SortedDurableQueue < DurableQueue
    @unacked = UnackQueue.new
    @ready = SortedReadyQueue.new
  end
end
