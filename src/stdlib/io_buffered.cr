module IO::Buffered
  def peek(size : Int)
    raise RuntimeError.new("Can't fill buffer when read_buffering is #{read_buffering?}") unless read_buffering?
    raise ArgumentError.new("size must be positive") unless size.positive?
    if size > @buffer_size
      raise ArgumentError.new("size (#{size}) can't be greater than buffer_size #{@buffer_size}")
    end

    # Enough data in buffer already
    return @in_buffer_rem[0, size] if size < @in_buffer_rem.size

    in_buffer = in_buffer()

    # Move data to beginning of in_buffer if needed
    if @in_buffer_rem.to_unsafe != in_buffer
      @in_buffer_rem.copy_to(in_buffer, @in_buffer_rem.size)
    end

    while @in_buffer_rem.size < size
      target = Slice.new(in_buffer + @in_buffer_rem.size, @buffer_size - @in_buffer_rem.size)
      bytes_read = unbuffered_read(target).to_i
      break if bytes_read.zero?
      @in_buffer_rem = Slice.new(in_buffer, @in_buffer_rem.size + bytes_read)
    end

    if @in_buffer_rem.size < size
      return @in_buffer_rem
    end

    @in_buffer_rem[0, size]
  end
end
