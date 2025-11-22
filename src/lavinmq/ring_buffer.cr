module LavinMQ
  class RingBuffer(T)
    @buffer : Pointer(T)
    @head = 0
    @size = 0
    @mask : Int32
    @capacity : Int32

    def initialize(capacity : Int32)
      raise ArgumentError.new("Capacity must be at least 2") if capacity < 2
      # Round up to next power of 2 for fast modulo via bitwise AND
      @capacity = Math.pw2ceil(capacity)
      if capacity != @capacity
        STDERR.puts "WARNING: RingBuffer capacity #{capacity} rounded up to #{@capacity} (must be power of 2)"
      end
      @mask = @capacity - 1
      @buffer = GC.malloc_atomic(@capacity * sizeof(T)).as(Pointer(T))
    end

    def push(value : T) : Nil
      tail = (@head + @size) & @mask
      @buffer[tail] = value

      if @size < @capacity
        @size += 1
      else
        @head = (@head + 1) & @mask
      end
    end

    def size : Int32
      @size
    end

    def [](index : Int32) : T
      raise IndexError.new if index >= @size || index < 0
      actual_index = (@head + index) & @mask
      @buffer[actual_index]
    end

    def to_a : Array(T)
      return Array(T).new if @size == 0

      result = Array(T).new(@size)
      tail = (@head + @size) & @mask

      if @size < @capacity || tail > @head
        # Not full yet, or full but not wrapped: copy single segment
        result.concat((@buffer + @head).to_slice(@size))
      else
        # Full and wrapped: copy two segments
        first_chunk_size = @capacity - @head
        result.concat((@buffer + @head).to_slice(first_chunk_size))
        result.concat(@buffer.to_slice(tail))
      end
      result
    end
  end
end
