module LavinMQ
  module Deduplication
    abstract class Cache(T)
      abstract def contains?(key : T) : Bool
      abstract def insert(key : T, ttl : UInt32?)
      abstract def delete(key : T)
      abstract def clear
    end

    class MemoryCache(T) < Cache(T)
      def initialize(size : UInt32? = nil)
        @size = size || 128_u32
        @lock = Mutex.new
        @store = Hash(T, Time::Instant?).new(initial_capacity: @size)
      end

      def contains?(key : T) : Bool
        @lock.synchronize do
          return false unless @store.has_key?(key)
          ttd = @store[key]
          return true unless ttd
          return true if ttd > RoughTime.instant
          @store.delete(key)
          false
        end
      end

      def insert(key : T, ttl : UInt32? = nil)
        @lock.synchronize do
          @store.shift if @store.size >= @size
          val = ttl.try { |v| RoughTime.instant + v.milliseconds }
          @store[key] = val
        end
      end

      def delete(key : T)
        @lock.synchronize { @store.delete(key) }
      end

      def clear
        @lock.synchronize { @store.clear }
      end
    end

    # Cache whose membership mirrors the messages currently in a queue.
    # Unlike MemoryCache it has no TTL or size bound: a key is present exactly
    # while a message bearing it is in the queue, and is removed (#delete) when
    # that message leaves. The +ttl+ argument is ignored.
    class SetCache(T) < Cache(T)
      def initialize
        @store = Set(T).new
      end

      def contains?(key : T) : Bool
        @store.includes?(key)
      end

      def insert(key : T, ttl : UInt32? = nil)
        @store.add(key)
      end

      # Atomically insert the key, returning true if it was newly added and
      # false if it was already present (i.e. a duplicate).
      def insert?(key : T, ttl : UInt32? = nil) : Bool
        @store.add?(key)
      end

      def delete(key : T)
        @store.delete(key)
      end

      def clear
        @store.clear
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

      def dedup_key(msg)
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
