require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true

    def initialize(vhost : VHost, name : String, args)
      args["x-overflow"] = "drop-head"
      super(vhost, name, false, false, args)
    end

    def head(c : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if c < 0
        stop = q_size + c - 1
        peek(0, stop, &blk)
      else
        stop = c - 1
        peek(0, stop, &blk)
      end
    end

    def tail(c : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if c < 0
        start = c.abs
        peek(start, q_size, &blk)
      else
        start = q_size - c
        peek(start, q_size, &blk)
      end
    end

    def peek(start : Int, stop : Int, &blk : SegmentPosition -> Nil)
      return if @ready.empty?
      start = 0 if start < 0
      @ready.each_with_index do |sp, i|
        next if i < start
        break if i > stop
        yield sp
      end
    end
  end
end
