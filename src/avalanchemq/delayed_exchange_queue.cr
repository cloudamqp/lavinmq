require "./queue"
require "./durable_queue"

module AvalancheMQ
  class DelayedExchangeQueue < Queue
    @ready = SortedReadyQueue.new
  end

  class DurableDelayedExchangeQueue < DurableQueue
    @ready = SortedReadyQueue.new

    def publish(sp : SegmentPosition, persistent = false) : Bool
      read(sp) do |env|
        delay = env.message.properties.headers.try(&.fetch("x-delay", nil)).try &.as(ArgumentNumber)
        @log.debug("Delaying message: #{delay}")
        sp = SegmentPosition.new(sp.segment, sp.position, sp.expiration_ts + delay) unless delay.nil?
      end
      super(sp, persistent)
    end
  end
end
