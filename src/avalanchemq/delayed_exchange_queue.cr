require "./queue"
require "./durable_queue"

module AvalancheMQ
  class DelayedExchangeQueue < Queue
    @ready = SortedReadyQueue.new
  end

  class DurableDelayedExchangeQueue < DurableQueue
    @ready = SortedReadyQueue.new

    def publish(sp : SegmentPosition, message : Message, persistent = false) : Bool
      delay = message.properties.headers.try(&.fetch("x-delay", nil)).try &.as(ArgumentNumber)
      @log.debug("DurableDelayedExchange#publish delaying message: #{delay}")
      sp = SegmentPosition.new(sp.segment, sp.position, message.timestamp + delay) unless delay.nil?
      super(sp, message, persistent)
    end
  end
end
