# Thread-safe IO implementations using Mutex + IO::Buffered
# Alternative to RingBufferIO for benchmarking different synchronization strategies
#
# Two variants:
# - MutexBlockingIO: Blocking via Channel signaling (fiber-friendly)
# - MutexSpinIO: Spin-yield-sleep backoff (like RingBufferIO but with mutex)
#
# Both use IO::Buffered for read/write buffering, with a simple SharedPipe
# for the underlying data transfer.
#
# Example:
#   reader, writer = LavinMQ.mutex_blocking_io_pair
#   spawn { writer.write(data); writer.flush }
#   spawn { reader.read(buffer) }

module LavinMQ
  # Factory for MutexBlockingIO pair
  def self.mutex_blocking_io_pair(buffer_size : Int32 = 8192) : {MutexBlockingIO, MutexBlockingIO}
    pipe_a_to_b = SharedPipe.new(buffer_size)
    pipe_b_to_a = SharedPipe.new(buffer_size)

    io_a = MutexBlockingIO.new(pipe_b_to_a, pipe_a_to_b)
    io_b = MutexBlockingIO.new(pipe_a_to_b, pipe_b_to_a)

    {io_a, io_b}
  end

  # Factory for MutexSpinIO pair
  def self.mutex_spin_io_pair(buffer_size : Int32 = 8192) : {MutexSpinIO, MutexSpinIO}
    pipe_a_to_b = SharedPipe.new(buffer_size)
    pipe_b_to_a = SharedPipe.new(buffer_size)

    io_a = MutexSpinIO.new(pipe_b_to_a, pipe_a_to_b)
    io_b = MutexSpinIO.new(pipe_a_to_b, pipe_b_to_a)

    {io_a, io_b}
  end

  # Simple mutex-protected pipe for data transfer between fibers
  # This is intentionally minimal - IO::Buffered handles the buffering
  class SharedPipe
    @data : IO::Memory
    @mutex : Mutex
    @closed : Bool = false
    @capacity : Int32

    # For blocking mode - channel-based signaling
    @data_waiters : Array(Channel(Nil))
    @space_waiters : Array(Channel(Nil))

    def initialize(@capacity : Int32 = 65536)
      @data = IO::Memory.new(@capacity)
      @mutex = Mutex.new
      @data_waiters = [] of Channel(Nil)
      @space_waiters = [] of Channel(Nil)
    end

    def close
      @mutex.synchronize do
        @closed = true
        @data_waiters.each { |ch| ch.send(nil) rescue nil }
        @space_waiters.each { |ch| ch.send(nil) rescue nil }
        @data_waiters.clear
        @space_waiters.clear
      end
    end

    def closed? : Bool
      @mutex.synchronize { @closed }
    end

    def readable_size : Int32
      @mutex.synchronize { @data.pos.to_i32 }
    end

    def writable_size : Int32
      @mutex.synchronize { @capacity - @data.pos.to_i32 }
    end

    def empty? : Bool
      readable_size == 0
    end

    # Non-blocking write - returns bytes written (may be 0 if full)
    def write_nonblocking(slice : Bytes) : Int32
      return 0 if slice.empty?

      @mutex.synchronize do
        return 0 if @closed

        available = @capacity - @data.pos.to_i32
        return 0 if available == 0

        to_write = Math.min(slice.size, available)
        @data.write(slice[0, to_write])

        # Wake up one reader
        if waiter = @data_waiters.shift?
          waiter.send(nil) rescue nil
        end

        to_write
      end
    end

    # Non-blocking read - returns bytes read (may be 0 if empty)
    def read_nonblocking(slice : Bytes) : Int32
      return 0 if slice.empty?

      @mutex.synchronize do
        return 0 if @data.pos == 0

        # Read from beginning of buffer
        readable = @data.pos.to_i32
        to_read = Math.min(slice.size, readable)

        # Copy data out
        @data.rewind
        @data.read(slice[0, to_read])

        # Compact remaining data
        remaining = readable - to_read
        if remaining > 0
          # Move remaining data to front
          buffer = @data.to_slice
          buffer.copy_from((buffer + to_read).to_unsafe, remaining)
        end
        @data.pos = remaining

        # Wake up one writer
        if waiter = @space_waiters.shift?
          waiter.send(nil) rescue nil
        end

        to_read
      end
    end

    # Blocking write - blocks until space available or closed
    def write_blocking(slice : Bytes) : Int32
      return 0 if slice.empty?

      loop do
        wait_channel : Channel(Nil)? = nil

        result = @mutex.synchronize do
          return 0 if @closed

          available = @capacity - @data.pos.to_i32
          if available > 0
            to_write = Math.min(slice.size, available)
            @data.write(slice[0, to_write])

            if waiter = @data_waiters.shift?
              waiter.send(nil) rescue nil
            end

            to_write
          else
            wait_channel = Channel(Nil).new
            @space_waiters << wait_channel
            -1
          end
        end

        return result if result >= 0

        if ch = wait_channel
          ch.receive rescue nil
        end
      end
    end

    # Blocking read - blocks until data available or closed
    def read_blocking(slice : Bytes) : Int32
      return 0 if slice.empty?

      loop do
        wait_channel : Channel(Nil)? = nil

        result = @mutex.synchronize do
          if @data.pos > 0
            readable = @data.pos.to_i32
            to_read = Math.min(slice.size, readable)

            @data.rewind
            @data.read(slice[0, to_read])

            remaining = readable - to_read
            if remaining > 0
              buffer = @data.to_slice
              buffer.copy_from((buffer + to_read).to_unsafe, remaining)
            end
            @data.pos = remaining

            if waiter = @space_waiters.shift?
              waiter.send(nil) rescue nil
            end

            to_read
          elsif @closed
            0
          else
            wait_channel = Channel(Nil).new
            @data_waiters << wait_channel
            -1
          end
        end

        return result if result >= 0

        if ch = wait_channel
          ch.receive rescue nil
        end
      end
    end
  end

  # IO implementation using Mutex + Channel blocking
  # Uses IO::Buffered for read/write buffering
  class MutexBlockingIO < IO
    include IO::Buffered

    @read_pipe : SharedPipe
    @write_pipe : SharedPipe
    @closed : Bool = false

    def initialize(@read_pipe : SharedPipe, @write_pipe : SharedPipe)
      self.sync = true # Disable write buffering for immediate visibility
    end

    def closed? : Bool
      @closed
    end

    # IO::Buffered abstract methods - these are the "unbuffered" operations
    # that IO::Buffered will call when it needs to fill/flush its internal buffers

    def unbuffered_read(slice : Bytes) : Int32
      raise IO::Error.new("Read from closed IO") if @closed
      @read_pipe.read_blocking(slice)
    end

    def unbuffered_write(slice : Bytes) : Nil
      raise IO::Error.new("Write to closed IO") if @closed
      remaining = slice
      while remaining.size > 0
        raise IO::Error.new("Pipe closed") if @write_pipe.closed?
        written = @write_pipe.write_blocking(remaining)
        break if written == 0 && @write_pipe.closed?
        remaining = remaining[written..]
      end
    end

    def unbuffered_flush : Nil
      # No-op - data is immediately visible
    end

    def unbuffered_close : Nil
      return if @closed
      @closed = true
      @write_pipe.close
      @read_pipe.close
    end

    def unbuffered_rewind : Nil
      raise IO::Error.new("Cannot rewind")
    end

    # Additional methods to match RingBufferIO interface

    def available : Int32
      @read_pipe.readable_size
    end

    def writable : Int32
      @write_pipe.writable_size
    end
  end

  # IO implementation using Mutex + spin-yield-sleep backoff
  # Uses IO::Buffered for read/write buffering
  class MutexSpinIO < IO
    include IO::Buffered

    SPIN_LIMIT  =    10
    YIELD_COUNT =   100
    MAX_WAIT_NS = 1_000

    @read_pipe : SharedPipe
    @write_pipe : SharedPipe
    @closed : Bool = false

    def initialize(@read_pipe : SharedPipe, @write_pipe : SharedPipe)
      self.sync = true # Disable write buffering for immediate visibility
    end

    def closed? : Bool
      @closed
    end

    # IO::Buffered abstract methods

    def unbuffered_read(slice : Bytes) : Int32
      raise IO::Error.new("Read from closed IO") if @closed

      # Fast path
      bytes_read = @read_pipe.read_nonblocking(slice)
      return bytes_read if bytes_read > 0

      # Slow path - spin-yield-sleep
      spin_count = 0
      yield_count = 0

      loop do
        return 0 if @read_pipe.closed? && @read_pipe.empty?

        bytes_read = @read_pipe.read_nonblocking(slice)
        return bytes_read if bytes_read > 0

        spin_count += 1
        if spin_count < SPIN_LIMIT
          Intrinsics.pause
        else
          yield_count += 1
          if yield_count < YIELD_COUNT
            Fiber.yield
          else
            sleep(MAX_WAIT_NS.nanoseconds)
          end
        end
      end
    end

    def unbuffered_write(slice : Bytes) : Nil
      raise IO::Error.new("Write to closed IO") if @closed

      remaining = slice
      spin_count = 0
      yield_count = 0

      while remaining.size > 0
        raise IO::Error.new("Pipe closed") if @write_pipe.closed?

        written = @write_pipe.write_nonblocking(remaining)
        if written > 0
          remaining = remaining[written..]
          spin_count = 0
          yield_count = 0
          next
        end

        spin_count += 1
        if spin_count < SPIN_LIMIT
          Intrinsics.pause
        else
          yield_count += 1
          if yield_count < YIELD_COUNT
            Fiber.yield
          else
            sleep(MAX_WAIT_NS.nanoseconds)
          end
        end
      end
    end

    def unbuffered_flush : Nil
      # No-op
    end

    def unbuffered_close : Nil
      return if @closed
      @closed = true
      @write_pipe.close
      @read_pipe.close
    end

    def unbuffered_rewind : Nil
      raise IO::Error.new("Cannot rewind")
    end

    # Additional methods to match RingBufferIO interface

    def available : Int32
      @read_pipe.readable_size
    end

    def writable : Int32
      @write_pipe.writable_size
    end
  end
end
