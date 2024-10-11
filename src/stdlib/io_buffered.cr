module IO::Buffered
  def peek(size : Int)
    raise RuntimeError.new("Can't fill buffer when read_buffering is #{read_buffering?}") unless read_buffering?
    raise ArgumentError.new("size must be positive") unless size.positive?

    if size > @buffer_size
      raise ArgumentError.new("size (#{size}) can't be greater than buffer_size #{@buffer_size}")
    end

    remaining_capacity = @buffer_size - @in_buffer_rem.size
    return @in_buffer_rem[0, size] unless remaining_capacity.positive?

    in_buffer = in_buffer()

    while @in_buffer_rem.size < size
      target = Slice.new(in_buffer + @in_buffer_rem.size, remaining_capacity)
      read_size = unbuffered_read(target).to_i
      remaining_capacity -= read_size
      @in_buffer_rem = Slice.new(in_buffer, @buffer_size - remaining_capacity)
    end

    @in_buffer_rem[0, size]
  end
end
