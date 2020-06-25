require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true

    def initialize(vhost : VHost, name : String, args)
      args["x-overflow"] = "drop-head"
      super(vhost, name, false, false, args)
      unless @message_ttl.nil?
        spawn expire_loop, name: "PersistentExchangeQueue#expire_loop #{@vhost.name}/#{@name}"
      end
    end

    def expire_loop
      loop do
        ttl = time_to_message_expiration
        if ttl
          select
          when timeout ttl
            expire_messages
          end
        else
          @message_available.receive
        end
      rescue Channel::ClosedError
        break
      rescue ex
        @log.error { "Unexpected exception in expire_loop: #{ex.inspect_with_backtrace}" }
      end
    end

    def head(c : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if c < 0
        count = q_size + c
        return if count < 0
        @ready.each(0, count, &blk)
      else
        @ready.each(0, c, &blk)
      end
    end

    def tail(c : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if c < 0
        @ready.each(c.abs, q_size, &blk)
      else
        @ready.each(Math.max(0, q_size - c), q_size, &blk)
      end
    end

    def from(offset : Int64, &blk : SegmentPosition -> Nil)
      return @ready.each(&blk) if offset == 0
      start_sp = SegmentPosition.from_i64(offset)
      i = @ready.bsearch_index { |sp| sp >= start_sp } || 0
      @ready.each(i, @ready.size, &blk)
    end
  end
end
