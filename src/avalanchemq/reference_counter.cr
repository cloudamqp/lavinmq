module AvalancheMQ
  # A reference counter which performs an action
  # when the counter goes down to zero again
  class ZeroReferenceCounter(T)
    def initialize(&blk : T -> Nil)
      @on_zero = blk
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
        if v = @counter.fetch(k, nil)
          cnt = @counter[k] = v - 1
          if cnt.zero?
            @counter.delete k
            @on_zero.call k
          end
          cnt
        else
          raise KeyError.new("Missing key #{k}")
        end
      end
    end

    def size
      @counter.size
    end

    def capacity
      @counter.capacity
    end
  end
end
