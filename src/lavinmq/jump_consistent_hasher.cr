require "./hasher"

# Jump Consistent Hash implementation.
# Based on the paper "A Fast, Minimal Memory, Consistent Hash Algorithm"
# by John Lamping and Eric Veach (Google): https://arxiv.org/abs/1406.2294
#
# Provides:
# - Perfect distribution (equal load across buckets)
# - Minimal disruption (only 1/n keys move when adding nth bucket)
# - O(ln n) time complexity
# - No memory overhead for storing a ring
class JumpConsistentHasher(T) < Hasher(T)
  # Tracks targets and their weights for add/remove operations
  private record TargetEntry(T), key : String, weight : UInt32, target : T

  def initialize
    @entries = Array(TargetEntry(T)).new
    # Expanded bucket list where each target appears weight times
    @buckets = Array(T).new
  end

  def add(key : String, weight : UInt32, target : T)
    @entries << TargetEntry(T).new(key, weight, target)
    rebuild_buckets
  end

  def remove(key : String, weight : UInt32)
    @entries.reject! { |e| e.key == key && e.weight == weight }
    rebuild_buckets
  end

  def get(key : String) : T?
    return nil if @buckets.empty?
    return @buckets.first if @buckets.size == 1

    bucket_index = jump_hash(hash_key64(key), @buckets.size)
    @buckets[bucket_index]
  end

  # Rebuild the bucket list from entries.
  # Each target appears in the bucket list 'weight' times for weighted distribution.
  private def rebuild_buckets
    @buckets.clear
    @entries.each do |entry|
      entry.weight.times { @buckets << entry.target }
    end
  end

  # Jump Consistent Hash algorithm.
  # Maps a 64-bit key to a bucket index in [0, num_buckets).
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
