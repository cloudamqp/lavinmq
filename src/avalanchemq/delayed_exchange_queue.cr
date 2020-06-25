require "./queue"
require "./durable_queue"

module AvalancheMQ
  module DelayedExchangeQueuePublishMixin
    @ready : Queue::ReadyQueue = Queue::SortedReadyQueue.new

    def publish(sp : SegmentPosition, message : Message, persistent = false) : Bool
      delay = message.properties.headers.try(&.fetch("x-delay", nil)).try &.as(Queue::ArgumentNumber)
      @log.debug { "DelayedExchangeQueuePublishMixin#publish delaying message: #{delay}" }
      sp = SegmentPosition.new(sp.segment, sp.position, message.timestamp + delay) if delay
      was_not_empty = !empty?
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
