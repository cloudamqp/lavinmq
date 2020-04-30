module AvalancheMQ
  class ReferenceCounter(T)
    def initialize
      @counter = Hash(T, UInt32).new { 0_u32 }
    end

    def inc(v : T) : UInt32
      @counter[v] += 1
    end

    def dec(v : T) : UInt32
      @counter[v] -= 1
    end

    def each
      @counter.each do |k, v|
        yield k, v
      end
    end

    def each
      @counter.each
    end

    # Deletes all zero referenced keys
    def gc!
      @counter.delete_if { |_, v| v.zero? }
    end

    def size
      @counter.size
    end

    def capacity
      @counter.capacity
    end

    def clear
      @counter.clear
    end
  end

  # A reference counter which performs an action
  # when the counter goes down to zero again
  class ZeroReferenceCounter(T)
    def initialize(&blk : T -> Nil)
      @on_zero = blk
      @counter = Hash(T, UInt32).new { 0_u32 }
      @lock = Mutex.new(:unchecked)
    end

    def inc(v : T) : UInt32
      @lock.synchronize do
        @counter[v] += 1
      end
    end

    def dec(v : T) : UInt32
      @lock.synchronize do
        cnt = @counter[v] -= 1
        if cnt.zero?
          @counter.delete v
          @on_zero.call v
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

    def rehash
      @counter.rehash
    end
  end
end
