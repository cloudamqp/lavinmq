require "./socket_read_nonblock"
require "./io_buffered_discard"

lib LibC
  {% if flag?(:linux) %}
    TCP_CORK = 3
  {% end %}
end

# A pool of reusable byte buffers for IO::Buffered.
#
# Most connections are idle most of the time. By returning read/write buffers
# to a pool when they're not in use, memory can be reused across connections
# instead of each connection holding two buffers forever.
#
# Each thread has its own cache of buffers (a LIFO stack, so the most recently
# used buffer is reused first), so acquiring and releasing takes no locks.
# A buffer may be released on another thread than it was acquired on, it then
# goes into that thread's cache. Each cache keeps at most
# `CACHE_BYTES_PER_THREAD` worth of buffers, more are left to the GC.
class IO::BufferPool
  # Idle buffers kept per thread and pool: 256 buffers of the default 16 KiB.
  # With hundreds of busy connections, about 350 buffers were in use at once,
  # and 64 buffers made the pool drop and reallocate some of them.
  CACHE_BYTES_PER_THREAD = 4 * 1024 * 1024

  # :nodoc:
  class Cache
    getter buffers : Array(Pointer(UInt8))
    property allocated = 0_i64
    property reused = 0_i64
    property released = 0_i64
    property dropped = 0_i64

    def initialize(capacity : Int32)
      @buffers = Array(Pointer(UInt8)).new(capacity)
    end
  end

  getter buffer_size : Int32
  # Max number of buffers cached per thread
  getter max_cached : Int32
  getter id : Int32
  # All threads' caches for this pool, for stats
  @caches = Array(Cache).new
  @caches_lock = Mutex.new(:unchecked)

  # Per thread caches, indexed by pool id. Thread locals aren't scanned by
  # the GC, so the arrays are also kept in @@all_thread_caches.
  @[ThreadLocal]
  @@thread_caches : Array(Cache?)?
  @@all_thread_caches = Array(Array(Cache?)).new
  @@all_thread_caches_lock = Mutex.new(:unchecked)

  @@next_id = Atomic(Int32).new(0)

  def initialize(@buffer_size : Int32)
    @id = @@next_id.add(1, :relaxed)
    @max_cached = Math.max(1, CACHE_BYTES_PER_THREAD // @buffer_size)
  end

  # Acquire a buffer from the current thread's cache, or allocate a new one
  def acquire : Pointer(UInt8)
    cache = thread_cache
    if buffer = cache.buffers.pop?
      cache.reused += 1
      return buffer
    end
    cache.allocated += 1
    GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
  end

  # Return a buffer to the current thread's cache. If the cache is full, the
  # buffer is left for the GC.
  def release(buffer : Pointer(UInt8)) : Nil
    return if buffer.null?
    cache = thread_cache
    if cache.buffers.size < @max_cached
      cache.buffers.push(buffer)
      cache.released += 1
    else
      cache.dropped += 1
    end
  end

  # The current thread's cache. Creating it takes locks, which may suspend
  # the fiber and resume it on another thread, so the thread local is looked
  # up again after, and only a cache found without yielding is returned.
  # acquire and release don't yield, so they use it on its own thread.
  private def thread_cache : Cache
    loop do
      if (caches = @@thread_caches) && (cache = caches[@id]?)
        return cache
      end
      install_thread_cache
    end
  end

  private def install_thread_cache : Nil
    caches = @@thread_caches || register_thread_caches
    while caches.size <= @id
      caches << nil
    end
    # nothing from the thread local read to here yields, so a thread gets
    # exactly one cache, and only installed caches are registered
    return if caches[@id]
    cache = caches[@id] = Cache.new(@max_cached)
    @caches_lock.synchronize { @caches << cache }
  end

  private def register_thread_caches : Array(Cache?)
    caches = Array(Cache?).new
    @@all_thread_caches_lock.synchronize { @@all_thread_caches << caches }
    @@thread_caches ||= caches
  end

  def stats
    caches = @caches_lock.synchronize { @caches.dup }
    {
      buffer_size: @buffer_size,
      threads:     caches.size,
      available:   caches.sum(&.buffers.size),
      allocated:   caches.sum(&.allocated),
      reused:      caches.sum(&.reused),
      released:    caches.sum(&.released),
      dropped:     caches.sum(&.dropped),
    }
  end

  @@pools = Hash(Int32, IO::BufferPool).new
  @@pools_lock = Mutex.new(:unchecked)

  # Returns the pool for the given buffer size. Pools for other sizes (from
  # before a config reload) keep serving the connections that use them, their
  # caches are bounded by `CACHE_BYTES_PER_THREAD`.
  def self.for(buffer_size : Int32) : IO::BufferPool
    @@pools_lock.synchronize do
      @@pools[buffer_size] ||= IO::BufferPool.new(buffer_size)
    end
  end

  def self.all : Array(IO::BufferPool)
    @@pools_lock.synchronize { @@pools.values }
  end
end

# Opt-in buffer pooling for IO::Buffered, enabled with `#buffer_pool=`.
#
# - Write buffer: acquired on first write, released after each flush
# - Read buffer: released when all buffered data has been consumed. Sockets
#   only acquire a buffer once data is available, so idle sockets waiting
#   for data hold no read buffer at all.
module IO::Buffered
  # These overrides copy parts of the stdlib's IO::Buffered, re-verify them,
  # and bump the version here, when upgrading Crystal. IOs without a buffer
  # pool use the stdlib's implementation (previous_def).
  {% unless compare_versions(Crystal::VERSION, "1.21.0") >= 0 && compare_versions(Crystal::VERSION, "1.23.0") < 0 %}
    {% warning "IO::Buffered buffer pool overrides are only tested with Crystal 1.21 and 1.22, not #{Crystal::VERSION.id}" %}
  {% end %}

  @buffer_pool : IO::BufferPool? = nil

  getter buffer_pool

  # Use buffers from *pool* (also sets `buffer_size` to the pool's buffer size).
  # Must be set before any buffer has been allocated.
  def buffer_pool=(pool : IO::BufferPool?)
    self.buffer_size = pool.buffer_size if pool
    @buffer_pool = pool
  end

  # Pooled buffers are released while not in use, so the stdlib's check for
  # allocated buffers doesn't stop a change to a size the pool's buffers
  # don't have
  def buffer_size=(value)
    if (pool = @buffer_pool) && value != pool.buffer_size
      raise ArgumentError.new("Cannot change buffer_size of an IO using a buffer pool")
    end
    previous_def
  end

  def read(slice : Bytes) : Int32
    return previous_def unless @buffer_pool
    # A buffer emptied by read_byte, peek or skip is returned before a read
    # that may wait for data
    release_in_buffer if @in_buffer_rem.empty?
    count = previous_def
    release_in_buffer if @in_buffer_rem.empty?
    count
  end

  # TCP_CORK is set, see #write
  @corked = false

  # A write that doesn't fit what's left of the buffer tops the buffer up
  # first, so that the buffered bytes go out with the start of it instead of
  # as a small write of their own. Those writes happen before the caller's
  # flush, and with TCP_NODELAY each would be sent as a TCP segment of its
  # own, so a TCP socket is corked until that flush, which sends whatever is
  # left right away. A write that fits the buffer doesn't cork.
  def write(slice : Bytes) : Nil
    return previous_def if @buffer_pool.nil? || sync? || flush_on_newline?
    check_open
    return if slice.empty?
    count = @out_count
    space = @buffer_size - count
    if slice.size <= space
      slice.copy_to(out_buffer + count, slice.size)
      @out_count = count + slice.size
      return
    end
    cork
    if count > 0
      slice[0, space].copy_to(out_buffer + count, space)
      @out_count = @buffer_size
      slice += space
      write_out_buffer
    end
    if slice.size >= @buffer_size
      unbuffered_write(slice)
    else
      slice.copy_to(out_buffer, slice.size)
      @out_count = slice.size
    end
  end

  private def cork : Nil
    return if @corked
    if socket = self.as?(TCPSocket)
      set_tcp_cork(socket, 1)
      @corked = true
    end
  end

  private def uncork : Nil
    return unless @corked
    @corked = false
    if socket = self.as?(TCPSocket)
      set_tcp_cork(socket, 0)
    end
  end

  private def set_tcp_cork(socket : TCPSocket, value : Int32) : Nil
    {% if flag?(:linux) %}
      LibC.setsockopt(socket.fd, LibC::IPPROTO_TCP, LibC::TCP_CORK, pointerof(value), sizeof(Int32))
    {% end %}
  end

  def flush : self
    return previous_def unless @buffer_pool
    write_out_buffer
    unbuffered_flush
    uncork
    self
  end

  # The write buffer is detached before writing, so that a concurrent flush or
  # close sees nothing to write, instead of writing (or releasing) a buffer
  # that may already be back in the pool and in use by another connection.
  # On a failed write the buffered data is dropped, the connection is
  # abandoned then anyway.
  private def write_out_buffer : Nil
    pool = @buffer_pool || return
    out_buf = @out_buffer
    return if out_buf.null?
    count = @out_count
    @out_buffer = Pointer(UInt8).null
    @out_count = 0
    begin
      unbuffered_write(Slice.new(out_buf, count)) if count > 0
    ensure
      pool.release(out_buf)
    end
  end

  def discard_write_buffer : Nil
    previous_def
    if (pool = @buffer_pool) && (out_buf = @out_buffer)
      @out_buffer = Pointer(UInt8).null
      pool.release(out_buf)
    end
  end

  # Flushes through the pool, so that the write buffer is detached and
  # returned like on any flush, then detaches the IO from the pool. A reader
  # that still holds the read buffer returns it when its read fails.
  def close : Nil
    return previous_def unless @buffer_pool
    begin
      flush if @out_count > 0
    ensure
      @buffer_pool = nil
      previous_def
    end
  end

  private def out_buffer
    if pool = @buffer_pool
      @out_buffer ||= pool.acquire
    else
      previous_def
    end
  end

  # The reader releases with the pool it acquired from, as a concurrent close
  # detaches the IO from the pool while the reader still holds the buffer
  private def release_in_buffer(pool = @buffer_pool) : Nil
    if pool && (in_buf = @in_buffer)
      @in_buffer = Pointer(UInt8).null
      @in_buffer_rem = Bytes.empty
      pool.release(in_buf)
    end
  end

  private def fill_buffer
    return previous_def unless pool = @buffer_pool
    {% if flag?(:unix) %}
      if socket = self.as?(Socket)
        return fill_socket_buffer(socket, pool)
      end
    {% end %}
    in_buffer = (@in_buffer ||= pool.acquire)
    begin
      size = unbuffered_read(Slice.new(in_buffer, @buffer_size)).to_i
    rescue ex
      release_in_buffer(pool)
      raise ex
    end
    @in_buffer_rem = Slice.new(in_buffer, size)
    release_in_buffer(pool) if size.zero?
  end

  # Non-blocking read into a pooled buffer. If no data is available the buffer
  # is returned to the pool while waiting for the socket to become readable.
  #
  # Assumes one reading fiber per socket, like LavinMQ's client read loops.
  # Unlike a blocking read, the wait doesn't hold the fd's read lock, so with
  # several readers waiting, the event loop wakes one of them, and data that
  # one leaves unread doesn't wake the others.
  private def fill_socket_buffer(socket : Socket, pool : IO::BufferPool) : Nil
    loop do
      in_buffer = (@in_buffer ||= pool.acquire)
      begin
        size = socket.read_nonblock(Slice.new(in_buffer, @buffer_size))
      rescue ex
        release_in_buffer(pool)
        raise ex
      end
      if size
        @in_buffer_rem = Slice.new(in_buffer, size)
        release_in_buffer(pool) if size.zero?
        return
      end
      @in_buffer = Pointer(UInt8).null
      pool.release(in_buffer)
      socket.wait_readable
    end
  end
end
