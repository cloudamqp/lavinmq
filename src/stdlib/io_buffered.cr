module IO::Buffered
  def peek(size : Int)
    raise RuntimeError.new("Can't fill buffer when read_buffering is #{read_buffering?}") unless read_buffering?
    raise ArgumentError.new("size must be positive") unless size.positive?

    if size > @buffer_size
      raise ArgumentError.new("size (#{size}) can't be greater than buffer_size #{@buffer_size}")
    end

    return @in_buffer_rem[0, size] if size < @in_buffer_rem.size

    remaining_capacity = @buffer_size - @in_buffer_rem.size
    in_buffer = in_buffer()

    if @in_buffer_rem.to_unsafe != in_buffer
      @in_buffer_rem.copy_to(in_buffer, @in_buffer_rem.size)
    end

    while @in_buffer_rem.size < size
      target = Slice.new(in_buffer + @in_buffer_rem.size, remaining_capacity)
      bytes_read = unbuffered_read(target).to_i
      raise IO::Error.new("io closed?") if bytes_read.zero?
      remaining_capacity -= bytes_read
      @in_buffer_rem = Slice.new(in_buffer, @buffer_size - remaining_capacity)
    end

    @in_buffer_rem[0, size]
  end
end
