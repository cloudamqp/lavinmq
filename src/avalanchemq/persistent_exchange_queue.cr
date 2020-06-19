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
        sleep 1.seconds
        consumer_or_expire
      end
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

    def all(&blk : SegmentPosition -> Nil)
      peek(0, @ready.size, &blk)
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
