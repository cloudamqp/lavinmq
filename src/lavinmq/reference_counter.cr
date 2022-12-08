module LavinMQ
  # A reference counter which performs an action
  # when the counter goes down to zero again
  class ZeroReferenceCounter(T)
    def initialize(&on_zero : T -> Nil)
      @on_zero = on_zero
      @counter = Hash(T, UInt32).new
      @lock = Mutex.new(:unchecked)
    end

    def []=(k : T, v : Int)
      @lock.synchronize do
        @counter[k] = v.to_u32
      end
    end

    def inc(k : T) : UInt32
      @lock.synchronize do
        v = @counter.fetch(k, 0_u32)
        @counter[k] = v + 1
      end
    end

    def dec(k : T) : UInt32
      @lock.synchronize do
        v = @counter[k]
        cnt = v - 1
        if cnt.zero?
          @counter.delete k
          @on_zero.call k
        else
          @counter[k] = cnt
        end
        cnt
      end
    end

    def size
      @counter.size
    end

    def capacity
      @counter.capacity
    end

    def has_key?(k : T)
      @counter.has_key? k
    end
  end
end
