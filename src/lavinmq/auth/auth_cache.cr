module LavinMQ
  class CacheEntry(T)
    getter value : T
    getter expires_at : Time

    def initialize(@value : T, ttl : Time::Span)
      @expires_at = Time.utc + ttl
    end

    def expired? : Bool
      Time.utc > @expires_at
    end
  end

  class Cache(K, V)
    def initialize(@default_ttl : Time::Span = 1.hour)
      @mutex = Mutex.new
      @data = Hash(K, CacheEntry(V)).new
    end

    def set(key : K, value : V, ttl : Time::Span = @default_ttl) : V
      @mutex.synchronize do
        @data[key] = CacheEntry.new(value, ttl)
        value
      end
    end

    def get?(key : K) : V?
      @mutex.synchronize do
        entry = @data[key]?
        return nil unless entry

        if entry.expired?
          @data.delete(key)
          nil
        else
          entry.value
        end
      end
    end

    def delete(key : K) : Bool
      @mutex.synchronize do
        @data.delete(key) ? true : false
      end
    end

    def cleanup
      @mutex.synchronize do
        @data.reject! { |_, entry| entry.expired? }
      end
    end

    def clear
      @mutex.synchronize do
        @data.clear
      end
    end

    def size : Int32
      @mutex.synchronize do
        @data.size
      end
    end
  end
end
