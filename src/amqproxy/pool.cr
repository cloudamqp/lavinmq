module AMQProxy
  class Pool(T)
    def initialize(@max_size : Int32, min_size = 1, &create : -> T)
      @pool = Channel::Buffered(T).new(@max_size)
      @create = create
      @size = 0
      @borrowed = 0
      min_size.times do
        spawn increase_pool
      end
    end

    def increase_pool
      @size += 1
      @pool.send @create.call
    end

    def borrow(&block : T -> _)
      @borrowed += 1
      if @size <= @borrowed && @size < @max_size
        spawn increase_pool
      end

      s = @pool.receive
      block.call s
    ensure
      if s.nil?
        puts "Socket is nil"
        @size -= 1
      elsif s.closed?
        puts "Socket closed when returned"
        @size -= 1
      else
        @pool.send s
      end
      @borrowed -= 1
    end
  end
end
