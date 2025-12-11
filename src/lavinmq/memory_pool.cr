require "deque"

module LavinMQ
  # Generic thread-safe object pool for reducing allocation overhead
  # Inspired by Bun's thread-local memory pools
  class MemoryPool(T)
    Log = LavinMQ::Log.for "memory_pool"
    
    @pool = Deque(T).new
    @mutex = Mutex.new
    @max_size : Int32
    @total_created = Atomic(Int64).new(0)
    @total_reused = Atomic(Int64).new(0)
    @current_size = Atomic(Int32).new(0)
    
    def initialize(@max_size : Int32 = 1000, &@factory : -> T)
    end
    
    # Acquire an object from the pool or create a new one
    def acquire : T
      obj = nil
      @mutex.synchronize do
        obj = @pool.shift? if @pool.size > 0
      end
      
      if obj
        @total_reused.add(1, :relaxed)
        obj
      else
        @total_created.add(1, :relaxed)
        @current_size.add(1, :relaxed)
        @factory.call
      end
    end
    
    # Return an object to the pool
    def release(obj : T) : Nil
      @mutex.synchronize do
        if @pool.size < @max_size
          @pool.push(obj)
        else
          # Pool is full, let object be garbage collected
          @current_size.add(-1, :relaxed)
        end
      end
    end
    
    # Get pool statistics
    def stats
      {
        pool_size: @pool.size,
        max_size: @max_size,
        total_created: @total_created.get(:relaxed),
        total_reused: @total_reused.get(:relaxed),
        current_size: @current_size.get(:relaxed),
        reuse_rate: calculate_reuse_rate
      }
    end
    
    private def calculate_reuse_rate : Float64
      created = @total_created.get(:relaxed)
      reused = @total_reused.get(:relaxed)
      total = created + reused
      total > 0 ? (reused.to_f / total.to_f) * 100.0 : 0.0
    end
    
    # Clear the pool and reset statistics
    def clear : Nil
      @mutex.synchronize do
        size = @pool.size
        @pool.clear
        @current_size.add(-size, :relaxed)
      end
    end
  end
  
  # Specialized pool for IO::Memory buffers (byte arrays)
  # Reduces allocation churn for message processing
  class BufferPool
    Log = LavinMQ::Log.for "buffer_pool"
    
    @pools = Hash(Int32, MemoryPool(IO::Memory)).new
    @mutex = Mutex.new
    
    # Size buckets: 1KB, 4KB, 16KB, 64KB, 256KB, 1MB
    SIZE_BUCKETS = [1024, 4096, 16384, 65536, 262144, 1048576]
    
    def initialize(@max_per_bucket : Int32 = 500)
      SIZE_BUCKETS.each do |size|
        @pools[size] = MemoryPool(IO::Memory).new(@max_per_bucket) do
          IO::Memory.new(size)
        end
      end
    end
    
    # Get a buffer of at least the requested size
    def acquire(size : Int32) : IO::Memory
      bucket_size = find_bucket_size(size)
      
      if pool = @pools[bucket_size]?
        buffer = pool.acquire
        buffer.clear
        buffer
      else
        # Size too large for pooling
        IO::Memory.new(size)
      end
    end
    
    # Return a buffer to the pool
    def release(buffer : IO::Memory) : Nil
      capacity = buffer.capacity
      bucket_size = find_bucket_size(capacity)
      
      if pool = @pools[bucket_size]?
        # Only pool if buffer capacity matches bucket size
        if capacity == bucket_size
          buffer.clear
          pool.release(buffer)
        end
      end
    end
    
    # Find the appropriate bucket size for the requested size
    private def find_bucket_size(size : Int32) : Int32
      SIZE_BUCKETS.each do |bucket_size|
        return bucket_size if size <= bucket_size
      end
      size # Return original size if larger than all buckets
    end
    
    # Get statistics for all pools
    def stats : Hash(Int32, NamedTuple)
      result = Hash(Int32, NamedTuple).new
      @pools.each do |size, pool|
        result[size] = pool.stats
      end
      result
    end
    
    # Clear all pools
    def clear : Nil
      @pools.each_value(&.clear)
    end
  end
end
