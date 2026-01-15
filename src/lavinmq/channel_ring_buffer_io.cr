# Thread-safe, zero-syscall IO implementation using a ring buffer with channel-based blocking
# Optimized for Single-Producer Single-Consumer (SPSC) pattern
#
# Uses channels for blocking semantics similar to how sockets block when buffers are full/empty.
# Brief spin loop before blocking for low-latency scenarios.
#
# Example:
#   reader, writer = LavinMQ.channel_ring_buffer_io_pair(65536)
#   spawn { writer.write(data); writer.flush }
#   spawn { reader.read(buffer) }

require "./ring_buffer_io"

module LavinMQ
  # A pair of connected ChannelRingBufferIO endpoints for bidirectional communication
  def self.channel_ring_buffer_io_pair(buffer_size : Int32 = 65536) : {ChannelRingBufferIO, ChannelRingBufferIO}
    # Two ring buffers for bidirectional communication
    buffer_a_to_b = RingBuffer.new(buffer_size)
    buffer_b_to_a = RingBuffer.new(buffer_size)

    # Channels for blocking - shared between endpoints
    # a_to_b direction: io_a writes, io_b reads
    data_a_to_b = Channel(Nil).new(1)
    space_a_to_b = Channel(Nil).new(1)

    # b_to_a direction: io_b writes, io_a reads
    data_b_to_a = Channel(Nil).new(1)
    space_b_to_a = Channel(Nil).new(1)

    io_a = ChannelRingBufferIO.new(
      read_buffer: buffer_b_to_a,
      write_buffer: buffer_a_to_b,
      read_data_ch: data_b_to_a,
      read_space_ch: space_b_to_a,
      write_data_ch: data_a_to_b,
      write_space_ch: space_a_to_b
    )

    io_b = ChannelRingBufferIO.new(
      read_buffer: buffer_a_to_b,
      write_buffer: buffer_b_to_a,
      read_data_ch: data_a_to_b,
      read_space_ch: space_a_to_b,
      write_data_ch: data_b_to_a,
      write_space_ch: space_b_to_a
    )

    {io_a, io_b}
  end

  # IO wrapper around RingBuffer using channels for blocking
  # Similar to socket behavior: blocks when buffer full/empty instead of spinning
  class ChannelRingBufferIO < IO
    SPIN_COUNT = 50 # Brief spin before blocking on channel

    @read_buffer : RingBuffer
    @write_buffer : RingBuffer
    @read_data_ch : Channel(Nil)   # Signal: data available to read
    @read_space_ch : Channel(Nil)  # Signal: space available (peer can write)
    @write_data_ch : Channel(Nil)  # Signal: we wrote data (peer can read)
    @write_space_ch : Channel(Nil) # Signal: space available for us to write
    @write_closed : Bool = false
    @read_closed : Bool = false
    @sync : Bool = true

    property sync : Bool

    def initialize(
      @read_buffer : RingBuffer,
      @write_buffer : RingBuffer,
      @read_data_ch : Channel(Nil),
      @read_space_ch : Channel(Nil),
      @write_data_ch : Channel(Nil),
      @write_space_ch : Channel(Nil),
    )
    end

    def closed? : Bool
      @write_closed && @read_closed
    end

    def read(slice : Bytes) : Int32
      raise IO::Error.new("Read from closed ChannelRingBufferIO") if @read_closed
      return 0 if slice.empty?

      loop do
        # Fast path - data available
        bytes_read = @read_buffer.read(slice)
        if bytes_read > 0
          # Signal writer that space is available (non-blocking)
          signal_space_available
          return bytes_read
        end

        # EOF: peer closed their write side and buffer is empty
        return 0 if @read_buffer.closed? && @read_buffer.empty?

        # Brief spin before blocking
        SPIN_COUNT.times do
          Intrinsics.pause
          bytes_read = @read_buffer.read(slice)
          if bytes_read > 0
            signal_space_available
            return bytes_read
          end
          return 0 if @read_buffer.closed? && @read_buffer.empty?
        end

        # Block on channel until data is available
        # Drain any stale signal first, then wait
        @read_data_ch.receive?
        select
        when @read_data_ch.receive
          # Data signal received, loop to try read again
        when timeout(100.milliseconds)
          # Timeout to check for close periodically
        end
      end
    end

    def write(slice : Bytes) : Nil
      raise IO::Error.new("Write to closed ChannelRingBufferIO") if @write_closed
      return if slice.empty?

      remaining = slice

      while remaining.size > 0
        raise IO::Error.new("Write to closed ChannelRingBufferIO") if @write_buffer.closed?

        # Fast path - space available
        bytes_written = @write_buffer.write(remaining)
        if bytes_written > 0
          remaining = remaining[bytes_written..]
          # Signal reader that data is available (non-blocking)
          signal_data_available
          next
        end

        # Brief spin before blocking
        success = false
        SPIN_COUNT.times do
          Intrinsics.pause
          raise IO::Error.new("Write to closed ChannelRingBufferIO") if @write_buffer.closed?
          bytes_written = @write_buffer.write(remaining)
          if bytes_written > 0
            remaining = remaining[bytes_written..]
            signal_data_available
            success = true
            break
          end
        end
        next if success

        # Block on channel until space is available
        # Drain any stale signal first, then wait
        @write_space_ch.receive?
        select
        when @write_space_ch.receive
          # Space signal received, loop to try write again
        when timeout(100.milliseconds)
          # Timeout to check for close periodically
        end
      end
    end

    def peek : Bytes?
      return nil if @read_closed
      return nil if @read_buffer.closed? && @read_buffer.empty?
      @read_buffer.peek
    end

    def flush : Nil
      # No-op - data is immediately visible via memory barriers
    end

    def close : Nil
      return if @write_closed
      @write_closed = true
      @read_closed = true
      @write_buffer.close
      @read_buffer.close
      # Signal to unblock any waiting readers/writers
      signal_data_available
      signal_space_available
    end

    def available : Int32
      @read_buffer.readable_size
    end

    def writable : Int32
      @write_buffer.writable_size
    end

    # Non-blocking signal that data is available
    private def signal_data_available
      select
      when @write_data_ch.send(nil)
        # Signal sent
      else
        # Channel full, signal already pending - that's fine
      end
    end

    # Non-blocking signal that space is available
    private def signal_space_available
      select
      when @read_space_ch.send(nil)
        # Signal sent
      else
        # Channel full, signal already pending - that's fine
      end
    end
  end
end
