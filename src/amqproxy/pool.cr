module AMQProxy
  class Pool(T)
    def initialize(size, &create : -> T)
      @pool = Channel::Buffered(T).new(size)
      until @pool.full?
        @pool.send create.call
      end
    end

    def borrow(&block : T -> _)
      s = @pool.receive
      block.call s
    ensure
      if s.nil?
        puts "Socket is nil"
      elsif s.closed?
        puts "Socket closed when returned"
      else
        @pool.send s
      end
    end
  end
end
