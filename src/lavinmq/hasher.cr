require "digest/crc32"

# Abstract base class for consistent hash implementations.
# Provides a common interface for different hashing algorithms.
abstract class Hasher(T)
  # Add a target to the hasher with the given key and weight.
  # The weight determines how many "slots" or "virtual nodes" the target occupies.
  abstract def add(key : String, weight : UInt32, target : T)

  # Remove a target from the hasher using its key and weight.
  # The weight must match what was used when adding.
  abstract def remove(key : String, weight : UInt32)

  # Get the target for the given key, or nil if no targets exist.
  abstract def get(key : String) : T?

  # Hash a string key to a UInt32 value using CRC32.
  protected def hash_key(key : String) : UInt32
    Digest::CRC32.checksum(key)
  end

  # Hash a string key to a UInt64 value for algorithms requiring larger hash space.
  protected def hash_key64(key : String) : UInt64
    # Use two CRC32 hashes combined for a 64-bit hash
    high = Digest::CRC32.checksum(key).to_u64
    low = Digest::CRC32.checksum("#{key}_salt").to_u64
    (high << 32) | low
  end
end
