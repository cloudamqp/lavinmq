module AvalancheMQ
  class SparseArray(T)
    getter size

    def initialize(initial_capacity = 4)
      @store = Array(T | Nil).new(initial_capacity, nil)
      @size = 0
    end

    def []=(key, value)
      @size += 1
      while key > @store.size
        @store << nil
      end
      @store[key] = value
    end

    def [](key)
      @store[key].not_nil!
    end

    def []?(key)
      @store[key]
    end

    def delete(key)
      @size -= 1
      v = @store[key]
      @store[key] = nil
      v
    end

    def each_value
      @store.each.compact_map { |v| v }
    end

    def each_value(&blk : Client::Channel -> _)
      @store.each do |v|
        yield v unless v.nil?
      end
    end

    def clear
      @store.clear
    end
  end
end
