require "./queue"
require "./durable_queue"

module AvalancheMQ
  class DelayedExchangeQueue < Queue
    @ready = SortedReadyQueue.new
  end

  class DurableDelayedExchangeQueue < DurableQueue
    def initialize(*args)
      super
      @ready = SortedReadyQueue.new
    end

    def publish(sp : SegmentPosition, message : Message, persistent = false) : Bool
      delay = message.properties.headers.try(&.fetch("x-delay", nil)).try &.as(ArgumentNumber)
      @log.debug("DurableDelayedExchange#publish delaying message: #{delay}")
      sp = SegmentPosition.new(sp.segment, sp.position, message.timestamp + delay) unless delay.nil?
      was_empty = empty?
      if result = super(sp, message, persistent)
        refresh_ttl_timeout unless was_empty
      end
      result
    end
  end
end
