require "../lavinmq/buffer_pool"

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
  # Class-level buffer pools (one for read, one for write)
  @@in_buffer_pool : LavinMQ::BufferPool?
  @@out_buffer_pool : LavinMQ::BufferPool?

  # Initialize buffer pools with the specified buffer size
  def self.setup_buffer_pools(buffer_size : Int32, max_pool_size : Int32 = 100_000) : Nil
    @@in_buffer_pool = LavinMQ::BufferPool.new(buffer_size, max_pool_size)
    @@out_buffer_pool = LavinMQ::BufferPool.new(buffer_size, max_pool_size)
  end

  # Get combined stats from both pools
  def self.buffer_pool_stats
    in_pool = @@in_buffer_pool
    out_pool = @@out_buffer_pool
    return nil unless in_pool && out_pool

    in_stats = in_pool.stats
    out_stats = out_pool.stats
    {
      in_buffer:  in_stats,
      out_buffer: out_stats,
      total:      {
        available:   in_stats[:available] + out_stats[:available],
        allocated:   in_stats[:allocated] + out_stats[:allocated],
        reused:      in_stats[:reused] + out_stats[:reused],
        released:    in_stats[:released] + out_stats[:released],
        buffer_size: in_stats[:buffer_size],
      },
    }
  end

  # Override fill_buffer to only acquire buffer after data is available.
  # For sockets, we wait for readability first (without holding a buffer),
  # then acquire the buffer and perform the actual read.
  # This ensures idle connections don't hold read buffers.
  private def fill_buffer
    pool = @@in_buffer_pool
    use_pool = pool && @buffer_size == pool.buffer_size

    # For sockets: wait for data before acquiring buffer
    # This keeps idle connections from holding buffers
    if use_pool && self.is_a?(Socket)
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
      pool = @@in_buffer_pool
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
    pool = @@out_buffer_pool
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
    pool = @@out_buffer_pool
    @out_buffer ||= if pool && @buffer_size == pool.buffer_size
                      pool.acquire
                    else
                      GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
                    end
  end

  # Override close to release buffers back to pool
  def close : Nil
    # Release read buffer before close (write buffer released via flush)
    in_pool = @@in_buffer_pool
    if in_pool && @buffer_size == in_pool.buffer_size
      if in_buf = @in_buffer
        in_pool.release(in_buf)
        @in_buffer = Pointer(UInt8).null
        @in_buffer_rem = Bytes.empty
      end
    end

    flush if @out_count > 0
  ensure
    unbuffered_close
  end
end
