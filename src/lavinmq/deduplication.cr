module LavinMQ
  module Deduplication
    abstract class Cache(T)
      abstract def contains?(key : T) : Bool
      abstract def insert(key : T, ttl : UInt32?)
    end

    class MemoryCache(T) < Cache(T)
      def initialize(@size : UInt32)
        @store = Hash(T, Time::Span?).new(initial_capacity: @size)
      end

      def contains?(key : T) : Bool
        return false unless @store.has_key?(key)
        ttd = @store[key]
        return true unless ttd
        return true if ttd > RoughTime.monotonic
        @store.delete(key)
        false
      end

      def insert(key : T, ttl : UInt32? = nil)
        @store.shift if @store.size >= @size
        val = ttl.try { |v| RoughTime.monotonic + v.milliseconds }
        @store[key] = val
      end
    end

    class Deduper
      DEFAULT_HEADER_KEY = "x-deduplication-header"

      def initialize(@cache : Cache(AMQ::Protocol::Field), @default_ttl : UInt32? = nil,
                     @header_key : String? = nil)
      end

      def add(msg : Message)
        key = dedup_key(msg)
        return unless key
        @cache.insert(key, dedup_ttl(msg))
      end

      def duplicate?(msg : Message) : Bool
        key = dedup_key(msg)
        return false unless key
        @cache.contains?(key)
      end

      private def dedup_key(msg)
        headers = msg.properties.headers
        return unless headers
        key = @header_key || DEFAULT_HEADER_KEY
        headers[key]?
      end

      private def dedup_ttl(msg) : UInt32?
        headers = msg.properties.headers
        def_ttl = @default_ttl
        return def_ttl unless headers
        value = headers["x-cache-ttl"]?
        return def_ttl unless value
        value = value.try(&.as?(Int32))
        return def_ttl unless value
        value.to_u32 || def_ttl
      end
    end
  end
end
