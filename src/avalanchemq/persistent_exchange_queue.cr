require "./durable_queue"

module AvalancheMQ
  class PersistentExchangeQueue < DurableQueue
    @internal = true

    def initialize(vhost : VHost, name : String, args)
      super(vhost, name, false, false, args)
    end

    def head(c : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if c < 0
        start = 0
        stop = q_size + c
        peek(start, stop, &blk)
      else
        start = q_size - c
        peek(start, q_size, &blk)
      end
    end

    def tail(c : Int, &blk : SegmentPosition -> Nil)
      q_size = @ready.size
      if c < 0
        peek(c, q_size, &blk)
      else
        peek(0, c.abs, &blk)
      end
    end

    def peek(start : Int, stop : Int, &blk : SegmentPosition -> Nil)
      return if @ready.empty?
      start = 0 if start < 0
      i = -1
      @ready.each do |sp|
        i += 1
        next if i < start
        break if i > stop
        yield sp
      end
    end
  end
end
