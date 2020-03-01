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
  end
end
