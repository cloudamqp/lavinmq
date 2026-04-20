require "json"

module LavinMQWasm
  # IEEE 802.3 CRC32 — identical polynomial to Digest::CRC32 / zlib crc32().
  # Pure Crystal: no libz, no native deps, compiles to WASM.
  private CRC32_TABLE = begin
    arr = StaticArray(UInt32, 256).new(0_u32)
    256.times do |i|
      c = i.to_u32
      8.times { c = c & 1 == 1 ? 0xEDB88320_u32 ^ (c >> 1) : c >> 1 }
      arr[i] = c
    end
    arr
  end

  protected def self.crc32(str : String) : UInt32
    crc = 0xFFFFFFFF_u32
    str.each_byte { |b| crc = (crc >> 8) ^ CRC32_TABLE[(crc ^ b.to_u32) & 0xFF] }
    crc ^ 0xFFFFFFFF_u32
  end

  protected def self.crc32_64(str : String) : UInt64
    high = crc32(str).to_u64
    low = crc32("#{str}_salt").to_u64
    (high << 32) | low
  end

  # Ring consistent hasher (mirrors RingConsistentHasher from main source)
  private class RingHasher
    def initialize
      @ring = Hash(UInt32, String).new
      @sorted_keys = Array(UInt32).new
    end

    def add(name : String, weight : UInt32)
      weight.times do |t|
        @ring[LavinMQWasm.crc32("#{name}.#{t}")] = name
      end
      @sorted_keys = @ring.keys.sort!
    end

    def get(key : String) : String?
      return nil if @ring.empty?
      return @ring.first.last if @ring.size == 1
      h = LavinMQWasm.crc32(key)
      ring_key = @sorted_keys.bsearch { |x| x >= h }
      ring_key ? @ring[ring_key] : @ring.first.last
    end
  end

  # Jump consistent hasher (mirrors JumpConsistentHasher from main source)
  private class JumpHasher
    def initialize
      @entries = Array({String, UInt32}).new
      @buckets = Array(String).new
    end

    def add(name : String, weight : UInt32)
      @entries << {name, weight}
      rebuild
    end

    private def rebuild
      @buckets.clear
      @entries.each { |name, w| w.times { @buckets << name } }
    end

    def get(key : String) : String?
      return nil if @buckets.empty?
      return @buckets.first if @buckets.size == 1
      @buckets[jump_hash(LavinMQWasm.crc32_64(key), @buckets.size)]
    end

    private def jump_hash(key : UInt64, num_buckets : Int32) : Int32
      b : Int64 = -1
      j : Int64 = 0
      while j < num_buckets
        b = j
        key = key &* 2862933555777941757_u64 &+ 1
        j = ((b + 1).to_f64 * (1_i64 << 31).to_f64 / ((key >> 33) + 1).to_f64).to_i64
      end
      b.to_i32
    end
  end

  # bindings: Array of {queue_name, weight_string}
  # exchange_args: {"x-algorithm" => "ring"|"jump", "x-hash-on" => header_name}
  def self.route_consistent_hash(
    bindings : Array({String, String}),
    routing_key : String,
    message_headers : Hash(String, JSON::Any),
    exchange_args : Hash(String, JSON::Any),
  ) : Array(String)
    return [] of String if bindings.empty?

    algorithm = exchange_args["x-algorithm"]?.try(&.as_s?) || "ring"
    hash_on = exchange_args["x-hash-on"]?.try(&.as_s?)

    hasher : RingHasher | JumpHasher = algorithm == "jump" ? JumpHasher.new : RingHasher.new

    bindings.each do |queue, weight_str|
      hasher.add(queue, weight_str.to_u32? || 1_u32)
    end

    key = hash_on && !hash_on.empty? ? (message_headers[hash_on]?.try(&.as_s?) || "") : routing_key
    matched = hasher.get(key)
    matched ? [matched] : [] of String
  end
end
