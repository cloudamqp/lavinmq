# A thread-safe pool of reusable byte buffers for IO operations.
# This reduces memory usage for connections with many idle sockets by
# releasing buffers back to the pool when not actively in use.
#
# Buffers are acquired on-demand when reading/writing begins and
# released back to the pool when operations complete and the connection
# goes idle waiting for more data.
module LavinMQ
  class BufferPool
    Log = LavinMQ::Log.for("buffer_pool")

    @buffers = Deque(Pointer(UInt8)).new
    @mutex = Mutex.new
    @buffer_size : Int32
    @max_pool_size : Int32
    @stats_allocated = Atomic(Int64).new(0)
    @stats_reused = Atomic(Int64).new(0)
    @stats_released = Atomic(Int64).new(0)

    getter buffer_size

    def initialize(@buffer_size : Int32, @max_pool_size : Int32 = 100_000)
    end

    # Acquire a buffer from the pool, or allocate a new one if the pool is empty
    def acquire : Pointer(UInt8)
      buffer = @mutex.synchronize { @buffers.shift? }
      if buffer
        @stats_reused.add(1)
        buffer
      else
        @stats_allocated.add(1)
        GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
      end
    end

    # Release a buffer back to the pool for reuse
    # If the pool is at capacity, the buffer is discarded (GC will collect it)
    def release(buffer : Pointer(UInt8)) : Nil
      return if buffer.null?
      @stats_released.add(1)
      @mutex.synchronize do
        if @buffers.size < @max_pool_size
          @buffers.push(buffer)
        else
          # If the pool is full, let GC collect the buffer
          GC.free(buffer.as(Void*))
        end
      end
    end

    # Number of buffers currently available in the pool
    def available : Int32
      @mutex.synchronize { @buffers.size }
    end

    # Statistics about buffer usage
    def stats
      {
        available:   available,
        allocated:   @stats_allocated.get,
        reused:      @stats_reused.get,
        released:    @stats_released.get,
        buffer_size: @buffer_size,
      }
    end

    # Clear all buffers from the pool
    def clear : Nil
      @mutex.synchronize { @buffers.clear }
    end
  end
end
