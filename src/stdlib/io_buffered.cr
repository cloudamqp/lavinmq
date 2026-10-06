require "socket"

lib LibC
  {% if flag?(:linux) %}
    MSG_DONTWAIT = 0x40
  {% else %}
    MSG_DONTWAIT = 0x80
  {% end %}
end

# A thread-safe pool of reusable byte buffers for IO::Buffered.
#
# Most connections are idle most of the time. By returning read/write buffers
# to a shared pool when they're not in use, memory can be reused across
# connections instead of each connection holding two buffers forever.
class IO::BufferPool
  getter buffer_size : Int32
  @buffers = Deque(Pointer(UInt8)).new
  @lock = Mutex.new(:unchecked)
  @allocated = Atomic(Int64).new(0)
  @reused = Atomic(Int64).new(0)
  @released = Atomic(Int64).new(0)

  def initialize(@buffer_size : Int32, @max_pooled : Int32 = 10_000)
  end

  # Acquire a buffer from the pool, or allocate a new one if the pool is empty
  def acquire : Pointer(UInt8)
    if buffer = @lock.synchronize { @buffers.shift? }
      @reused.add(1, :relaxed)
      buffer
    else
      @allocated.add(1, :relaxed)
      GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
    end
  end

  # Return a buffer to the pool. If the pool is full the buffer is left for the GC.
  def release(buffer : Pointer(UInt8)) : Nil
    return if buffer.null?
    @released.add(1, :relaxed)
    @lock.synchronize do
      @buffers.push(buffer) if @buffers.size < @max_pooled
    end
  end

  def available : Int32
    @lock.synchronize { @buffers.size }
  end

  def stats
    {
      buffer_size: @buffer_size,
      available:   available,
      allocated:   @allocated.get(:relaxed),
      reused:      @reused.get(:relaxed),
      released:    @released.get(:relaxed),
    }
  end

  @@pools = Hash(Int32, IO::BufferPool).new
  @@pools_lock = Mutex.new(:unchecked)

  # Returns the shared pool for the given buffer size
  def self.for(buffer_size : Int32) : IO::BufferPool
    @@pools_lock.synchronize do
      @@pools[buffer_size] ||= IO::BufferPool.new(buffer_size)
    end
  end

  def self.each(&)
    pools = @@pools_lock.synchronize { @@pools.values }
    pools.each { |pool| yield pool }
  end
end

# Opt-in buffer pooling for IO::Buffered, enabled with `#buffer_pool=`.
#
# - Write buffer: acquired on first write, released after each flush
# - Read buffer: released when all buffered data has been consumed. Sockets
#   only acquire a buffer once data is available, so idle sockets waiting
#   for data hold no read buffer at all.
module IO::Buffered
  @buffer_pool : IO::BufferPool? = nil

  getter buffer_pool

  # Use buffers from *pool* (also sets `buffer_size` to the pool's buffer size).
  # Must be set before any buffer has been allocated.
  def buffer_pool=(pool : IO::BufferPool?)
    self.buffer_size = pool.buffer_size if pool
    @buffer_pool = pool
  end

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
    release_in_buffer if @in_buffer_rem.empty?
    to_read
  end

  def flush : self
    unbuffered_write(Slice.new(out_buffer, @out_count)) if @out_count > 0
    unbuffered_flush
    @out_count = 0
    if (pool = @buffer_pool) && (out_buf = @out_buffer)
      @out_buffer = Pointer(UInt8).null
      pool.release(out_buf)
    end
    self
  end

  private def release_in_buffer : Nil
    if (pool = @buffer_pool) && (in_buf = @in_buffer)
      @in_buffer = Pointer(UInt8).null
      @in_buffer_rem = Bytes.empty
      pool.release(in_buf)
    end
  end

  private def fill_buffer
    pool = @buffer_pool
    {% if flag?(:unix) %}
      if pool && (socket = self.as?(Socket))
        return fill_socket_buffer(socket, pool)
      end
    {% end %}
    in_buffer = (@in_buffer ||= pool ? pool.acquire : GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*))
    size = unbuffered_read(Slice.new(in_buffer, @buffer_size)).to_i
    @in_buffer_rem = Slice.new(in_buffer, size)
  end

  # Non-blocking read into a pooled buffer. If no data is available the buffer
  # is returned to the pool while waiting for the socket to become readable.
  private def fill_socket_buffer(socket : Socket, pool : IO::BufferPool) : Nil
    loop do
      in_buffer = (@in_buffer ||= pool.acquire)
      ret = LibC.recv(socket.fd, in_buffer, @buffer_size, LibC::MSG_DONTWAIT)
      if ret >= 0
        @in_buffer_rem = Slice.new(in_buffer, ret.to_i)
        return
      end
      case Errno.value
      when Errno::EAGAIN # same as EWOULDBLOCK on Linux and macOS
        @in_buffer = Pointer(UInt8).null
        pool.release(in_buffer)
        Crystal::EventLoop.current.wait_readable(socket)
        check_open
      when Errno::EINTR
        next
      else
        raise IO::Error.from_errno("read", target: self)
      end
    end
  end
end
