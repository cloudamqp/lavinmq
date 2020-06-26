require "./queue"
require "./durable_queue"

module AvalancheMQ
  module DelayedExchangeQueuePublishMixin
    @ready : Queue::ReadyQueue = Queue::SortedReadyQueue.new
    @internal = true

    def publish(sp : SegmentPosition, message : Message? = nil, persistent = false) : Bool
      message = message.not_nil!
      delay = message.properties.headers.try(&.fetch("x-delay", nil)).try &.as(ArgumentNumber)
      @log.debug { "DelayedExchangeQueuePublishMixin#publish delaying message: #{delay}" }
      sp = SegmentPosition.new(sp.segment, sp.position, message.timestamp + delay) if delay
      was_not_empty = any?
      result = super(sp, message, persistent)
      refresh_ttl_timeout if result && delay && was_not_empty
      result
    end
  end

  class DelayedExchangeQueue < Queue
    include DelayedExchangeQueuePublishMixin
  end

  class DurableDelayedExchangeQueue < DurableQueue
    include DelayedExchangeQueuePublishMixin
  end
end
