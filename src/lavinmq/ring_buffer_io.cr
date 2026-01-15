# Thread-safe, zero-syscall IO implementation using a ring buffer
# Optimized for Single-Producer Single-Consumer (SPSC) pattern
#
# Use case: Replace IO::Stapled.pipe or UNIXSocket.pair for inter-fiber
# communication without kernel involvement.
#
# Example:
#   reader, writer = RingBufferIO.pair(65536)
#   spawn { writer.write(data); writer.flush }
#   spawn { reader.read(buffer) }

module LavinMQ
  # A pair of connected RingBufferIO endpoints for bidirectional communication
  def self.ring_buffer_io_pair(buffer_size : Int32 = 65536) : {RingBufferIO, RingBufferIO}
    # Two ring buffers for bidirectional communication
    buffer_a_to_b = RingBuffer.new(buffer_size)
    buffer_b_to_a = RingBuffer.new(buffer_size)

    io_a = RingBufferIO.new(buffer_b_to_a, buffer_a_to_b) # reads from b_to_a, writes to a_to_b
    io_b = RingBufferIO.new(buffer_a_to_b, buffer_b_to_a) # reads from a_to_b, writes to b_to_a

    {io_a, io_b}
  end

  # Lock-free SPSC ring buffer
  # Uses atomic operations for thread-safety without syscalls
  class RingBuffer
    @buffer : Bytes
    @capacity : Int32
    @mask : Int32

    # Atomic indices - using separate cache lines would be better but Crystal doesn't expose that
    @head : Atomic(Int32) = Atomic(Int32).new(0) # Write position (producer)
    @tail : Atomic(Int32) = Atomic(Int32).new(0) # Read position (consumer)
    @closed : Atomic(Bool) = Atomic(Bool).new(false)

    def initialize(size : Int32)
      # Round up to power of 2 for efficient modulo via bitmask
      @capacity = next_power_of_2(size)
      @mask = @capacity - 1
      @buffer = Bytes.new(@capacity)
    end

    private def next_power_of_2(n : Int32) : Int32
      return 1 if n <= 0
      n -= 1
      n |= n >> 1
      n |= n >> 2
      n |= n >> 4
      n |= n >> 8
      n |= n >> 16
      n + 1
    end

    def close
      @closed.set(true)
    end

    def closed? : Bool
      @closed.get
    end

    # Available space for writing
    def writable_size : Int32
      head = @head.get(:acquire)
      tail = @tail.get(:acquire)
      @capacity &- (head &- tail)
    end

    # Available data for reading
    def readable_size : Int32
      head = @head.get(:acquire)
      tail = @tail.get(:acquire)
      head &- tail
    end

    def empty? : Bool
      readable_size == 0
    end

    def full? : Bool
      writable_size == 0
    end

    # Write data to the buffer (producer side)
    # Returns number of bytes written (may be less than slice.size if buffer full)
    def write(slice : Bytes) : Int32
      return 0 if slice.empty?

      head = @head.get(:relaxed)
      tail = @tail.get(:acquire)

      available = @capacity &- (head &- tail)
      return 0 if available == 0

      to_write = Math.min(slice.size, available)
      write_pos = head & @mask

      # Handle wrap-around
      first_chunk = Math.min(to_write, @capacity - write_pos)
      slice[0, first_chunk].copy_to(@buffer + write_pos)

      if to_write > first_chunk
        # Wrapped around, write remainder at start
        slice[first_chunk, to_write - first_chunk].copy_to(@buffer)
      end

      @head.set(head &+ to_write, :release)
      to_write
    end

    # Read data from the buffer (consumer side)
    # Returns number of bytes read (may be less than slice.size if buffer empty)
    def read(slice : Bytes) : Int32
      return 0 if slice.empty?

      tail = @tail.get(:relaxed)
      head = @head.get(:acquire)

      available = head &- tail
      return 0 if available == 0

      to_read = Math.min(slice.size, available)
      read_pos = tail & @mask

      # Handle wrap-around
      first_chunk = Math.min(to_read, @capacity - read_pos)
      (@buffer + read_pos).copy_to(slice.to_unsafe, first_chunk)

      if to_read > first_chunk
        # Wrapped around, read remainder from start
        @buffer.copy_to(slice.to_unsafe + first_chunk, to_read - first_chunk)
      end

      @tail.set(tail &+ to_read, :release)
      to_read
    end

    def peek : Bytes?
      tail = @tail.get(:relaxed)
      head = @head.get(:acquire)
      return nil if head == tail # No data available

      read_pos = tail & @mask
      available = head &- tail
      contiguous = Math.min(available, @capacity - read_pos)
      @buffer[read_pos, contiguous] # Zero-copy slice view
    end
  end

  # IO wrapper around RingBuffer for use as a drop-in replacement for pipes/sockets
  class RingBufferIO < IO
    SPIN_LIMIT  =    10 # Spin iterations before yielding
    YIELD_COUNT =   100 # Yields before longer sleep
    MAX_WAIT_NS = 1_000 # Max nanoseconds to sleep (1µs)

    @read_buffer : RingBuffer
    @write_buffer : RingBuffer
    @write_closed : Bool = false # We've closed our write side
    @read_closed : Bool = false  # We've stopped reading
    @sync : Bool = true          # Default to sync mode (no internal buffering)

    property sync : Bool

    def initialize(@read_buffer : RingBuffer, @write_buffer : RingBuffer)
    end

    # Returns true only when both read and write are closed
    def closed? : Bool
      @write_closed && @read_closed
    end

    def read(slice : Bytes) : Int32
      raise IO::Error.new("Read from closed RingBufferIO") if @read_closed
      return 0 if slice.empty?

      # Fast path - data available
      bytes_read = @read_buffer.read(slice)
      return bytes_read if bytes_read > 0

      # Slow path - wait for data
      spin_count = 0
      yield_count = 0

      loop do
        # EOF: peer closed their write side (our read buffer) and it's empty
        return 0 if @read_buffer.closed? && @read_buffer.empty?

        bytes_read = @read_buffer.read(slice)
        return bytes_read if bytes_read > 0

        # Backoff strategy
        spin_count += 1
        if spin_count < SPIN_LIMIT
          # Spin (CPU busy-wait) - fastest for very short waits
          Intrinsics.pause
        else
          yield_count += 1
          if yield_count < YIELD_COUNT
            # Yield to other fibers - cooperative multitasking
            Fiber.yield
          else
            # Sleep briefly - reduces CPU usage for longer waits
            sleep(MAX_WAIT_NS.nanoseconds)
          end
        end
      end
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("Write to closed RingBufferIO") if @write_closed
      return if slice.empty?

      remaining = slice
      spin_count = 0
      yield_count = 0

      while remaining.size > 0
        raise IO::Error.new("Write to closed RingBufferIO") if @write_buffer.closed?

        bytes_written = @write_buffer.write(remaining)
        if bytes_written > 0
          remaining = remaining[bytes_written..]
          spin_count = 0
          yield_count = 0
          next
        end

        # Buffer full, wait for space
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

    def peek : Bytes?
      return nil if @read_closed
      return nil if @read_buffer.closed? && @read_buffer.empty?
      @read_buffer.peek
    end

    def flush : Nil
      # No-op for sync mode, data is immediately visible
      # The ring buffer uses memory barriers for visibility
    end

    # Close this IO endpoint
    # - Signals EOF to peer (closes our write buffer)
    # - Stops accepting writes
    # - Reading still allowed until peer closes or buffer empty
    def close : Nil
      return if @write_closed
      @write_closed = true
      @read_closed = true
      @write_buffer.close
      @read_buffer.close
    end

    # Convenience: check how much data is available without blocking
    def available : Int32
      @read_buffer.readable_size
    end

    # Convenience: check how much space is available for writing
    def writable : Int32
      @write_buffer.writable_size
    end
  end
end
