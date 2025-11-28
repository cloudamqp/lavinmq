require "digest/crc32"

# Information on Consistent Hash Ring
# http://www.martinbroadhurst.com/Consistent-Hash-Ring.html
class ConsistentHasher(T)
  def initialize
    @ring = Hash(UInt32, T).new(initial_capacity: 128)
    @sorted_keys = Array(UInt32).new(128)
  end

  private def hash_key(key) : UInt32
    Digest::CRC32.checksum(key)
  end

  def add(key : String, weight : UInt32, target : T)
    weight.times do |t|
      @ring[hash_key("#{key}.#{t}")] = target
    end
    @sorted_keys = @ring.keys.sort!
  end

  def remove(key : String, weight : UInt32)
    weight.times do |t|
      @ring.delete(hash_key("#{key}.#{t}"))
    end
    @sorted_keys = @ring.keys.sort!
  end

  def get(key)
    size = @ring.size
    return if size.zero?
    return @ring.first.last if size == 1
    key_hash = hash_key(key)
    ring_hash = @sorted_keys.bsearch { |x| x >= key_hash }
    # Wrap around the ring and takes the first entry if no larger hash was found
    return @ring.first.last if ring_hash.nil?
    @ring[ring_hash]
  end
end
