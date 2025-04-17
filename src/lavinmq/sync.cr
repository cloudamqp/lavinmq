# A thread-safe hash implementation that synchronizes all operations with a mutex.
#
# `SyncHash` wraps a standard Crystal `Hash` and ensures all operations are
# performed atomically by using a `Mutex` for synchronization. This makes it
# safe to use in concurrent environments where multiple fibers or threads
# might access or modify the hash simultaneously.
#
# ## Features
# - Thread-safe access to all hash operations
# - Support for initialization with default values or from existing hashes
# - Atomic update operations for common concurrent patterns
# - Safe iteration through protected snapshots
#
# ## Example
# ```
# # Create a synchronized hash
# counters = SyncHash(String, Int32).new
# counters["requests"] = 0
#
# # Use in multiple fibers
# 10.times do
#   spawn do
#     100.times do
#       counters.update_if_exists("requests") { |value| value + 1 }
#     end
#   end
# end
#
# # Wait for fibers to complete
# sleep 1
#
# # Should output 1000
# puts counters["requests"]
# ```
class SyncHash(K, V)
  # Internal hash and mutex
  @mutex = Mutex.new
  @hash = Hash(K, V).new

  # Initialize with optional default block
  #
  # ```
  # # Create a hash with default values
  # scores = SyncHash(String, Int32).new { 0 }
  # puts scores["unknown_player"] # => 0
  # ```
  def initialize(&block : K -> V)
    @hash = Hash(K, V).new(&block)
  end

  # Initialize with a regular hash
  #
  # ```
  # # Create from an existing hash
  # initial_data = {"one" => 1, "two" => 2}
  # sync_hash = SyncHash(String, Int32).new(initial_data)
  # ```
  def initialize(hash : Hash(K, V))
    @hash = hash.dup
  end

  # Default empty initializer
  #
  # ```
  # # Create an empty hash
  # cache = SyncHash(String, String).new
  # ```
  def initialize
    @hash = Hash(K, V).new
  end

  # --- Core operations ---

  # Get a value for a key
  #
  # Raises `KeyError` if the key does not exist.
  #
  # ```
  # hash = SyncHash(String, Int32).new
  # hash["key"] = 42
  # hash["key"]     # => 42
  # hash["missing"] # raises KeyError
  # ```
  def [](key : K) : V
    @mutex.synchronize do
      @hash[key]
    end
  end

  # Set a value for a key
  #
  # ```
  # hash = SyncHash(String, Int32).new
  # hash["counter"] = 1
  # hash["counter"] # => 1
  # ```
  def []=(key : K, value : V) : V
    @mutex.synchronize do
      @hash[key] = value
    end
  end

  # Get a value or return nil if key doesn't exist
  #
  # ```
  # hash = SyncHash(String, Int32).new
  # hash["key"] = 42
  # hash["key"]?     # => 42
  # hash["missing"]? # => nil
  # ```
  def []?(key : K) : V?
    @mutex.synchronize do
      @hash[key]?
    end
  end

  # Delete a key-value pair
  def delete(key : K) : V?
    @mutex.synchronize do
      @hash.delete(key)
    end
  end

  # Clear the hash
  def clear : self
    @mutex.synchronize do
      @hash.clear
    end
    self
  end

  # Returns the number of elements
  def size : Int32
    @mutex.synchronize do
      @hash.size
    end
  end

  # Check if hash is empty
  def empty? : Bool
    @mutex.synchronize do
      @hash.empty?
    end
  end

  # Check if hash has a key
  def has_key?(key : K) : Bool
    @mutex.synchronize do
      @hash.has_key?(key)
    end
  end

  # --- Collection operations ---

  # Get all keys
  def keys : Array(K)
    @mutex.synchronize do
      @hash.keys
    end
  end

  # Get all values
  def values : Array(V)
    @mutex.synchronize do
      @hash.values
    end
  end

  # Get a shallow copy of the hash
  def dup : Hash(K, V)
    @mutex.synchronize do
      @hash.dup
    end
  end

  # Thread-safe each iteration
  def each(&)
    # Make a copy to avoid locking during iteration
    snapshot = nil
    @mutex.synchronize do
      snapshot = @hash.dup
    end

    snapshot.each do |k, v|
      yield k, v
    end
  end

  # Thread-safe map
  def map(&block : K, V -> U) forall U
    result = [] of U
    each do |k, v|
      result << block.call(k, v)
    end
    result
  end

  # Thread-safe operation to update a value
  #
  # Updates an existing key's value atomically by applying the given block
  # to the current value. Returns the new value, or nil if the key doesn't exist.
  #
  # ```
  # counter = SyncHash(String, Int32).new
  # counter["visits"] = 0
  #
  # # Thread-safe increment
  # counter.update_if_exists("visits") { |value| value + 1 }  # => 1
  # counter.update_if_exists("missing") { |value| value + 1 } # => nil
  # ```
  def update_if_exists(key : K, &block : V -> V) : V?
    @mutex.synchronize do
      if @hash.has_key?(key)
        new_value = block.call(@hash[key])
        @hash[key] = new_value
        return new_value
      end
    end
    nil
  end

  # Atomically fetch and update a value
  #
  # Like `update_if_exists`, but passes both the key and value to the block.
  # Returns the new value, or nil if the key doesn't exist.
  #
  # ```
  # scores = SyncHash(String, Int32).new
  # scores["player1"] = 100
  #
  # # Conditionally update value
  # scores.compute_if_present("player1") do |key, score|
  #   puts "Updating #{key}"
  #   score + 50
  # end # => 150
  # ```
  def compute_if_present(key : K, &block : K, V -> V) : V?
    @mutex.synchronize do
      if @hash.has_key?(key)
        new_value = block.call(key, @hash[key])
        @hash[key] = new_value
        return new_value
      end
    end
    nil
  end

  # Atomically get or set a value
  #
  # Returns the existing value if the key exists, otherwise computes
  # a new value using the provided block, stores it, and returns it.
  # This operation is atomic and thread-safe.
  #
  # ```
  # cache = SyncHash(String, String).new
  #
  # # Only executes expensive operation once per key
  # value = cache.compute_if_absent("key") do |k|
  #   puts "Expensive computation for #{k}"
  #   "computed value"
  # end
  #
  # # On subsequent calls, returns cached value without running block
  # value = cache.compute_if_absent("key") do |k|
  #   puts "This won't be printed"
  #   "something else"
  # end
  # ```
  def compute_if_absent(key : K, &block : K -> V) : V
    @mutex.synchronize do
      if @hash.has_key?(key)
        return @hash[key]
      else
        new_value = block.call(key)
        @hash[key] = new_value
        return new_value
      end
    end
  end

  # String representation
  def to_s(io : IO) : Nil
    @mutex.synchronize do
      io << "SyncHash{"
      @hash.each_with_index do |(k, v), i|
        io << ", " if i > 0
        io << k << " => " << v
      end
      io << "}"
    end
  end

  # Inspect representation
  def inspect(io : IO) : Nil
    to_s(io)
  end
end

# Example usage:
# sync_hash = SyncHash(String, Int32).new
# sync_hash["counter"] = 0
#
# # Thread-safe increment
# 10.times do
#   spawn do
#     100.times do
#       sync_hash.update_if_exists("counter") { |value| value + 1 }
#     end
#   end
# end
#
# sleep 1
# puts sync_hash["counter"] # Should be 1000

# A thread-safe array implementation that synchronizes all operations with a mutex.
#
# `SyncArray` wraps a standard Crystal `Array` and ensures all operations are
# performed atomically by using a `Mutex` for synchronization. This makes it
# safe to use in concurrent environments where multiple fibers or threads
# might access or modify the array simultaneously.
#
# ## Features
# - Thread-safe access to all array operations
# - Support for initialization with default values or from existing arrays
# - Atomic update operations for common concurrent patterns
# - Safe iteration through protected snapshots
class SyncArray(T)
  # Internal array and mutex
  @mutex = Mutex.new
  @array : Array(T)

  # Initialize an empty array
  def initialize
    @array = Array(T).new
  end

  # Initialize with the given capacity
  def initialize(capacity : Int32)
    @array = Array(T).new(capacity)
  end

  # Initialize with default value
  def initialize(size : Int32, value : T)
    @array = Array(T).new(size, value)
  end

  # Initialize from an existing array
  def initialize(other : Array(T))
    @array = other.dup
  end

  # --- Core operations ---

  # Returns the element at the given index
  #
  # Raises `IndexError` if the index is out of bounds.
  def [](index : Int) : T
    @mutex.synchronize do
      @array[index]
    end
  end

  # Returns the element at the given index, or nil if out of bounds
  def []?(index : Int) : T?
    @mutex.synchronize do
      @array[index]?
    end
  end

  # Sets the element at the given index
  #
  # Raises `IndexError` if the index is out of bounds.
  def []=(index : Int, value : T) : T
    @mutex.synchronize do
      @array[index] = value
    end
  end

  # Appends a value to the array
  def <<(value : T) : self
    @mutex.synchronize do
      @array << value
    end
    self
  end

  # Returns the size of the array
  def size : Int32
    @mutex.synchronize do
      @array.size
    end
  end

  # Returns true if the array is empty
  def empty? : Bool
    @mutex.synchronize do
      @array.empty?
    end
  end

  # Removes all elements from the array
  def clear : self
    @mutex.synchronize do
      @array.clear
    end
    self
  end

  # Removes and returns the last element of the array
  #
  # Raises `IndexError` if the array is empty.
  def pop : T
    @mutex.synchronize do
      @array.pop
    end
  end

  # Removes and returns the last element of the array, or nil if empty
  def pop? : T?
    @mutex.synchronize do
      @array.pop?
    end
  end

  # Removes and returns the first element of the array
  #
  # Raises `IndexError` if the array is empty.
  def shift : T
    @mutex.synchronize do
      @array.shift
    end
  end

  # Removes and returns the first element of the array, or nil if empty
  def shift? : T?
    @mutex.synchronize do
      @array.shift?
    end
  end

  # Adds an element to the beginning of the array
  def unshift(value : T) : self
    @mutex.synchronize do
      @array.unshift(value)
    end
    self
  end

  # Returns true if the array includes the given element
  def includes?(value : T) : Bool
    @mutex.synchronize do
      @array.includes?(value)
    end
  end

  # --- Collection operations ---

  # Thread-safe each iteration
  #
  # Iterates over each element in the array. Makes a copy to
  # avoid locking during iteration.
  def each(&)
    # Make a copy to avoid locking during iteration
    snapshot = nil
    @mutex.synchronize do
      snapshot = @array.dup
    end

    snapshot.each do |value|
      yield value
    end
  end

  # Thread-safe map operation
  #
  # Returns a new array with the results of calling the block once
  # for every element in the array.
  def map(&block : T -> U) forall U
    result = [] of U
    each do |value|
      result << block.call(value)
    end
    result
  end

  # Thread-safe select operation
  #
  # Returns a new array containing all elements for which the block returns true.
  def select(&block : T -> Bool) : Array(T)
    result = [] of T
    each do |value|
      result << value if block.call(value)
    end
    result
  end

  # Atomically updates the element at the given index
  #
  # Raises `IndexError` if the index is out of bounds.
  def update_at(index : Int, &block : T -> T) : T
    @mutex.synchronize do
      new_value = block.call(@array[index])
      @array[index] = new_value
      new_value
    end
  end

  # Atomically updates the element at the given index if it exists
  #
  # Returns the new value, or nil if the index is out of bounds.
  def update_at?(index : Int, &block : T -> T) : T?
    @mutex.synchronize do
      if index >= 0 && index < @array.size
        new_value = block.call(@array[index])
        @array[index] = new_value
        return new_value
      end
    end
    nil
  end

  # Get a shallow copy of the array
  def to_a : Array(T)
    @mutex.synchronize do
      @array.dup
    end
  end

  # String representation
  def to_s(io : IO) : Nil
    @mutex.synchronize do
      io << "SyncArray["
      @array.each_with_index do |value, i|
        io << ", " if i > 0
        value.inspect(io)
      end
      io << "]"
    end
  end

  # Inspect representation
  def inspect(io : IO) : Nil
    to_s(io)
  end
end

# Example usage:
# thread_safe = SyncArray(Int32).new
# thread_safe << 1 << 2 << 3
#
# # Thread-safe operations
# 10.times do
#   spawn do
#     thread_safe << Random.rand(100)
#     thread_safe.update_at?(0) { |v| v + 1 } if thread_safe.size > 0
#   end
# end
#
# sleep 1
# puts thread_safe.to_a
