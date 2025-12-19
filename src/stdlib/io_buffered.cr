require "socket"

# A thread-safe pool of reusable byte buffers for IO operations.
# This reduces memory usage for connections with many idle sockets by
# releasing buffers back to the pool when not actively in use.
class IO::BufferPool
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

  # Singleton pool instance
  class_property pool : IO::BufferPool?

  def self.setup(buffer_size : Int32, max_pool_size : Int32 = 100_000) : Nil
    @@pool = IO::BufferPool.new(buffer_size, max_pool_size)
  end
end

# Override IO::Buffered to support buffer pooling for socket connections.
# This saves memory for idle connections by releasing buffers back to a pool
# when operations complete, allowing them to be reused by active connections.
#
# The key insight is that most connections are idle most of the time, waiting
# for data. By releasing buffers when:
# - Read buffer: all buffered data has been consumed
# - Write buffer: data has been flushed
#
# We can dramatically reduce memory usage for scenarios with many connections.
module IO::Buffered
  def self.buffer_pool_stats
    IO::BufferPool.pool.try &.stats
  end

  # Override fill_buffer to only acquire buffer after data is available.
  # For sockets, we release any existing buffer and wait for readability first,
  # then acquire the buffer and perform the actual read.
  # This ensures idle connections don't hold read buffers.
  private def fill_buffer
    pool = IO::BufferPool.pool
    use_pool = pool && @buffer_size == pool.buffer_size

    # For sockets: release buffer and wait for data before acquiring
    # This keeps idle connections from holding buffers
    if use_pool && self.is_a?(Socket)
      # Release existing buffer before waiting
      if (p = pool) && (in_buf = @in_buffer) && !in_buf.null?
        p.release(in_buf)
        @in_buffer = Pointer(UInt8).null
      end
      Crystal::EventLoop.current.wait_readable(self)
    end

    in_buf = if use_pool && (p = pool)
               @in_buffer ||= p.acquire
             else
               @in_buffer ||= GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
             end
    size = unbuffered_read(Slice.new(in_buf, @buffer_size)).to_i
    @in_buffer_rem = Slice.new(in_buf, size)
  end

  # Override read to release buffer back to pool when all data consumed
  def read(slice : Bytes) : Int32
    check_open

    count = slice.size
    return 0 if count == 0

    if @in_buffer_rem.empty?
      # If we are asked to read more than half the buffer's size,
      # read directly into the slice, as it's not worth the extra
      # memory copy.
      if !read_buffering? || count >= @buffer_size // 2
        return unbuffered_read(slice[0, count]).to_i
      else
        fill_buffer
        return 0 if @in_buffer_rem.empty?
      end
    end

    to_read = Math.min(count, @in_buffer_rem.size)
    slice.copy_from(@in_buffer_rem.to_unsafe, to_read)
    @in_buffer_rem += to_read

    # Release buffer back to pool when all data consumed
    if @in_buffer_rem.empty?
      pool = IO::BufferPool.pool
      if pool && @buffer_size == pool.buffer_size
        if in_buf = @in_buffer
          pool.release(in_buf)
          @in_buffer = Pointer(UInt8).null
        end
      end
    end

    to_read
  end

  # Override flush to release write buffer after flushing
  def flush : self
    if @out_count > 0
      unbuffered_write(Slice.new(out_buffer, @out_count))
    end
    unbuffered_flush
    @out_count = 0

    # Release write buffer back to pool after flush
    pool = IO::BufferPool.pool
    if pool && @buffer_size == pool.buffer_size
      if out_buf = @out_buffer
        pool.release(out_buf)
        @out_buffer = Pointer(UInt8).null
      end
    end

    self
  end

  # Override out_buffer to acquire from pool
  private def out_buffer
    pool = IO::BufferPool.pool
    @out_buffer ||= if pool && @buffer_size == pool.buffer_size
                      pool.acquire
                    else
                      GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
                    end
  end

  # Override close to release buffers back to pool
  def close : Nil
    # Release read buffer before close (write buffer released via flush)
    pool = IO::BufferPool.pool
    if pool && @buffer_size == pool.buffer_size
      if in_buf = @in_buffer
        pool.release(in_buf)
        @in_buffer = Pointer(UInt8).null
        @in_buffer_rem = Bytes.empty
      end
    end

    flush if @out_count > 0
  ensure
    unbuffered_close
  end
end
