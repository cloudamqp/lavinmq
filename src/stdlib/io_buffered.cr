require "./socket_read_nonblock"

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

  # Doesn't yield (which could move the fiber to another thread) unless the
  # current thread has no cache for this pool yet
  private def thread_cache : Cache
    if (caches = @@thread_caches) && (cache = caches[@id]?)
      return cache
    end
    new_thread_cache
  end

  private def new_thread_cache : Cache
    cache = Cache.new(@max_cached)
    @caches_lock.synchronize { @caches << cache }
    # the fiber may have moved to another thread while waiting for a lock,
    # so the thread local is read after taking them
    caches = @@thread_caches || register_thread_caches
    while caches.size <= @id
      caches << nil
    end
    caches[@id] ||= cache
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
  # These overrides copy parts of the stdlib's IO::Buffered, re-verify them,
  # and bump the version here, when upgrading Crystal. IOs without a buffer
  # pool use the stdlib's implementation (previous_def).
  {% unless compare_versions(Crystal::VERSION, "1.21.0") >= 0 && compare_versions(Crystal::VERSION, "1.22.0") < 0 %}
    {% warning "IO::Buffered buffer pool overrides are only tested with Crystal 1.21, not #{Crystal::VERSION.id}" %}
  {% end %}

  @buffer_pool : IO::BufferPool? = nil

  getter buffer_pool

  # Use buffers from *pool* (also sets `buffer_size` to the pool's buffer size).
  # Must be set before any buffer has been allocated.
  def buffer_pool=(pool : IO::BufferPool?)
    self.buffer_size = pool.buffer_size if pool
    @buffer_pool = pool
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

  def flush : self
    previous_def
    if (pool = @buffer_pool) && (out_buf = @out_buffer)
      @out_buffer = Pointer(UInt8).null
      pool.release(out_buf)
    end
    self
  end

  # Detach from the pool, buffers still held are left to the GC
  def close : Nil
    @buffer_pool = nil
    previous_def
  end

  private def out_buffer
    if pool = @buffer_pool
      @out_buffer ||= pool.acquire
    else
      previous_def
    end
  end

  private def release_in_buffer : Nil
    if (pool = @buffer_pool) && (in_buf = @in_buffer)
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
    size = unbuffered_read(Slice.new(in_buffer, @buffer_size)).to_i
    @in_buffer_rem = Slice.new(in_buffer, size)
  end

  # Non-blocking read into a pooled buffer. If no data is available the buffer
  # is returned to the pool while waiting for the socket to become readable.
  private def fill_socket_buffer(socket : Socket, pool : IO::BufferPool) : Nil
    loop do
      in_buffer = (@in_buffer ||= pool.acquire)
      if size = socket.read_nonblock(Slice.new(in_buffer, @buffer_size))
        @in_buffer_rem = Slice.new(in_buffer, size)
        release_in_buffer if size.zero?
        return
      end
      @in_buffer = Pointer(UInt8).null
      pool.release(in_buffer)
      socket.wait_readable
    end
  end
end
