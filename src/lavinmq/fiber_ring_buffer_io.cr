# Fiber-aware RingBufferIO that suspends/resumes fibers instead of spinning
# Uses atomic operations for thread-safety without mutex overhead
#
# Example:
#   reader, writer = LavinMQ.fiber_ring_buffer_io_pair
#   spawn { writer.write(data) }
#   spawn { reader.read(buffer) }

require "mutex"

module LavinMQ
  def self.fiber_ring_buffer_io_pair(buffer_size : Int32 = 8192) : {FiberRingBufferIO, FiberRingBufferIO}
    buffer_a_to_b = FiberRingBuffer.new(buffer_size)
    buffer_b_to_a = FiberRingBuffer.new(buffer_size)

    io_a = FiberRingBufferIO.new(buffer_b_to_a, buffer_a_to_b)
    io_b = FiberRingBufferIO.new(buffer_a_to_b, buffer_b_to_a)

    {io_a, io_b}
  end

  # Ring buffer that suspends/resumes fibers for blocking operations
  # Uses atomics to avoid race conditions without mutex overhead
  class FiberRingBuffer
    @buffer : Bytes
    @capacity : Int32
    @mask : Int32
    @head : Atomic(Int32) = Atomic(Int32).new(0)
    @tail : Atomic(Int32) = Atomic(Int32).new(0)
    @closed : Atomic(Bool) = Atomic(Bool).new(false)
    SPIN_LIMIT = 1

    # Waiting fibers (nil = no waiter)
    @reader : Fiber? = nil
    @writer : Fiber? = nil

    @reader_lock : Mutex = Mutex.new
    @writer_lock : Mutex = Mutex.new

    def initialize(size : Int32)
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
      # Log.info { "FiberRingBufferIO close called from fiber:#{Fiber.current}" }
      @closed.set(true, :release)
      # Wake up any waiting fibers
      wake_reader
      wake_writer
    end

    def closed? : Bool
      @closed.get(:acquire)
    end

    def readable_size : Int32
      @head.get(:acquire) &- @tail.get(:acquire)
    end

    def writable_size : Int32
      @capacity &- (@head.get(:acquire) &- @tail.get(:acquire))
    end

    def empty? : Bool
      readable_size == 0
    end

    def full? : Bool
      writable_size == 0
    end

    # Write data, suspending if buffer is full
    def write(slice : Bytes) : Int32
      return 0 if slice.empty?

      total_written = 0
      remaining = slice
      spin_count = 0

      while remaining.size > 0
        return total_written if @closed.get(:acquire)

        head = @head.get(:relaxed)
        tail = @tail.get(:acquire)
        available = @capacity &- (head &- tail)
        wakeup_reader = available == @capacity

        if available > 0
          to_write = Math.min(remaining.size, available)
          write_to_buffer(remaining, to_write, head)
          @head.set(head &+ to_write, :release)
          total_written += to_write
          remaining = remaining[to_write..]
          spin_count = 0
          # Wake up waiting reader AFTER each write
          # if wakeup_reader
          wake_reader
        else
          current_tail = tail
          while spin_count < SPIN_LIMIT
            spin_count += 1
            tail = @tail.get(:relaxed)
            break if current_tail != tail
          end
          next if current_tail != tail

          suspend = false
          @writer_lock.synchronize do
            # Re-check after setting waiter (avoid race condition)
            tail = @tail.get(:acquire)
            available = @capacity &- (head &- tail)
            if available > 0 || @closed.get(:acquire)
              next
            end
            suspend = true
            @writer = Fiber.current
          end
          # Log.debug { "FiberRingBufferIO write suspending fiber #{Fiber.current}" }
          if suspend
            Fiber.suspend
          end
          # Log.debug { "FiberRingBufferIO write resumed fiber #{Fiber.current}" }
        end
      end

      total_written
    end

    # Read data, suspending if buffer is empty
    def read(slice : Bytes) : Int32
      return 0 if slice.empty?

      spin_count = 0
      loop do
        tail = @tail.get(:relaxed)
        head = @head.get(:acquire)
        available = head &- tail
        wakeup_writer = available == @capacity

        if available > 0
          to_read = Math.min(slice.size, available)
          read_from_buffer(slice, to_read, tail)
          @tail.set(tail &+ to_read, :release)

          spin_count = 0
          # Wake up waiting writer
          #  if wakeup_writer
          wake_writer

          return to_read
        elsif @closed.get(:acquire)
          return 0 # EOF
        else
          current_head = head
          while spin_count < SPIN_LIMIT
            spin_count += 1
            head = @head.get(:relaxed)
            break if current_head != head
          end
          next if current_head != head

          suspend = false
          @reader_lock.synchronize do
            # Buffer empty - register as waiter and suspend
            head = @head.get(:acquire)
            available = head &- tail
            if available > 0 || @closed.get(:acquire)
              # Data arrived or closed, clear waiter and retry
              next
            end
            suspend = true
            @reader = Fiber.current
          end
          # Log.debug { "FiberRingBufferIO read suspending fiber #{Fiber.current}" }
          if suspend
            Fiber.suspend
          end
          # Log.debug { "FiberRingBufferIO read resumed fiber #{Fiber.current}" }
        end
      end
    end

    def peek : Bytes?
      tail = @tail.get(:acquire)
      head = @head.get(:acquire)
      available = head &- tail
      return nil if available == 0

      read_pos = tail & @mask
      contiguous = Math.min(available, @capacity - read_pos)
      @buffer[read_pos, contiguous]
    end

    # Drain buffer contents directly to an IO without intermediate buffer
    # Returns bytes written, 0 on EOF
    def drain_to(io : IO) : Int32
      spin_count = 0
      loop do
        tail = @tail.get(:relaxed)
        head = @head.get(:acquire)
        available = head &- tail

        if available > 0
          read_pos = tail & @mask
          # Calculate contiguous chunk (may wrap around)
          first_chunk = Math.min(available, @capacity - read_pos)

          # Write first chunk directly to IO
          io.write(@buffer[read_pos, first_chunk])
          total_written = first_chunk

          # If data wraps around, write second chunk
          if available > first_chunk
            second_chunk = available - first_chunk
            io.write(@buffer[0, second_chunk])
            total_written += second_chunk
          end

          @tail.set(tail &+ total_written, :release)
          wake_writer
          return total_written
        elsif @closed.get(:acquire)
          return 0 # EOF
        else
          current_head = head
          while spin_count < SPIN_LIMIT
            spin_count += 1
            head = @head.get(:relaxed)
            break if current_head != head
          end
          next if current_head != head

          suspend = false
          @reader_lock.synchronize do
            head = @head.get(:acquire)
            available = head &- tail
            if available > 0 || @closed.get(:acquire)
              next
            end
            suspend = true
            @reader = Fiber.current
          end
          Fiber.suspend if suspend
        end
      end
    end

    # Fill buffer directly from an IO without intermediate buffer
    # Returns bytes read, 0 on EOF from source IO
    def fill_from(io : IO) : Int32
      spin_count = 0
      loop do
        head = @head.get(:relaxed)
        tail = @tail.get(:acquire)
        available = @capacity &- (head &- tail)

        if available > 0
          write_pos = head & @mask
          # Calculate contiguous writable space (may wrap around)
          first_chunk = Math.min(available, @capacity - write_pos)

          # Read first chunk directly from IO
          bytes_read = io.read(@buffer[write_pos, first_chunk])
          return 0 if bytes_read == 0 # EOF from source

          total_read = bytes_read

          # If we filled the first chunk and there's more space, try second chunk
          if bytes_read == first_chunk && available > first_chunk
            second_chunk = available - first_chunk
            bytes_read2 = io.read(@buffer[0, second_chunk])
            total_read += bytes_read2
          end

          @head.set(head &+ total_read, :release)
          wake_reader
          return total_read
        elsif @closed.get(:acquire)
          return 0
        else
          current_tail = tail
          while spin_count < SPIN_LIMIT
            spin_count += 1
            tail = @tail.get(:relaxed)
            break if current_tail != tail
          end
          next if current_tail != tail

          suspend = false
          @writer_lock.synchronize do
            tail = @tail.get(:acquire)
            available = @capacity &- (head &- tail)
            if available > 0 || @closed.get(:acquire)
              next
            end
            suspend = true
            @writer = Fiber.current
          end
          Fiber.suspend if suspend
        end
      end
    end

    private def write_to_buffer(slice : Bytes, count : Int32, head : Int32)
      write_pos = head & @mask
      first_chunk = Math.min(count, @capacity - write_pos)
      slice[0, first_chunk].copy_to(@buffer + write_pos)

      if count > first_chunk
        slice[first_chunk, count - first_chunk].copy_to(@buffer)
      end
    end

    private def read_from_buffer(slice : Bytes, count : Int32, tail : Int32)
      read_pos = tail & @mask
      first_chunk = Math.min(count, @capacity - read_pos)
      (@buffer + read_pos).copy_to(slice.to_unsafe, first_chunk)

      if count > first_chunk
        @buffer.copy_to(slice.to_unsafe + first_chunk, count - first_chunk)
      end
    end

    # Atomically get and clear the waiting reader, then enqueue it
    private def wake_reader
      # Log.debug { "FiberRingBufferIO wake_reader called from: #{Fiber.current}" }
      @reader_lock.synchronize do
        if fiber = @reader
          # Log.debug { "FiberRingBufferIO waking reader fiber #{fiber}" }
          fiber.enqueue
          @reader = nil
        end
      end
    end

    # Atomically get and clear the waiting writer, then enqueue it
    private def wake_writer
      # Log.debug { "FiberRingBufferIO wake_writer called from: #{Fiber.current}" }
      @writer_lock.synchronize do
        if fiber = @writer
          # Log.debug { "FiberRingBufferIO waking writer fiber #{fiber}" }
          fiber.enqueue
          @writer = nil
        end
      end
    end
  end

  # IO wrapper for FiberRingBuffer
  class FiberRingBufferIO < IO
    @read_buffer : FiberRingBuffer
    @write_buffer : FiberRingBuffer
    @write_closed : Bool = false
    @read_closed : Bool = false
    @sync : Bool = true

    property sync : Bool

    def initialize(@read_buffer : FiberRingBuffer, @write_buffer : FiberRingBuffer)
    end

    def closed? : Bool
      # Fully closed when both directions are closed
      # Write is closed when we call close()
      # Read is closed when peer closes or we call close_both()
      @write_closed && (@read_closed || @read_buffer.closed?)
    end

    def read(slice : Bytes) : Int32
      # Allow reading even after we closed (to drain peer's remaining data)
      # Only error if explicitly told to stop reading via close_both
      raise IO::Error.new("Read from closed FiberRingBufferIO") if @read_closed
      return 0 if slice.empty?

      # If peer closed their write side (our read buffer), check for remaining data
      if @read_buffer.closed?
        return 0 if @read_buffer.empty? # EOF - no more data
      end

      @read_buffer.read(slice)
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("Write to closed FiberRingBufferIO") if @write_closed
      return if slice.empty?

      remaining = slice
      while remaining.size > 0
        raise IO::Error.new("Write to closed FiberRingBufferIO") if @write_buffer.closed?
        written = @write_buffer.write(remaining)
        raise IO::Error.new("Write to closed FiberRingBufferIO") if written == 0 && @write_buffer.closed?
        remaining = remaining[written..]
      end
    end

    def peek : Bytes?
      return nil if @read_closed
      return nil if @read_buffer.closed? && @read_buffer.empty?
      @read_buffer.peek
    end

    def flush : Nil
      # No-op - data is immediately visible
    end

    # Close this IO endpoint
    # Only closes the write side to signal EOF to peer
    # Read side stays open to drain any remaining data from peer
    def close : Nil
      # Log.info { "FiberRingBufferIO close called" }
      return if @write_closed
      @write_closed = true
      @write_buffer.close # Signal EOF to peer's read side
      raise IO::Error.new("Read from closed FiberRingBufferIO") if @read_closed
      # Note: @read_buffer is NOT closed - peer may still have data for us
      # @read_buffer will return EOF naturally when peer closes their write side
    end

    # Force close both directions (for cleanup)
    def close_both : Nil
      # Log.info { "FiberRingBufferIO close_both called" }
      return if @write_closed && @read_closed
      @write_closed = true
      @read_closed = true
      @write_buffer.close
      @read_buffer.close
    end

    def available : Int32
      @read_buffer.readable_size
    end

    def writable : Int32
      @write_buffer.writable_size
    end

    # Drain read buffer directly to an IO without intermediate buffer
    # Use this instead of read + io.write for zero-copy transfer
    # Returns bytes written, 0 on EOF
    def drain(io : IO) : Int32
      raise IO::Error.new("Read from closed FiberRingBufferIO") if @read_closed
      if @read_buffer.closed?
        return 0 if @read_buffer.empty?
      end
      @read_buffer.drain_to(io)
    end

    # Fill write buffer directly from an IO without intermediate buffer
    # Use this instead of io.read + write for zero-copy transfer
    # Returns bytes read, 0 on EOF from source
    def fill(io : IO) : Int32
      raise IO::Error.new("Write to closed FiberRingBufferIO") if @write_closed
      raise IO::Error.new("Write to closed FiberRingBufferIO") if @write_buffer.closed?
      @write_buffer.fill_from(io)
    end
  end
end
