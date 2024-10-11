module IO::Buffered
  def peek(size : Int)
    raise RuntimeError.new("Can't fill buffer when read_buffering is #{read_buffering?}") unless read_buffering?
    raise ArgumentError.new("size must be positive") unless size.positive?

    if size > @buffer_size
      raise ArgumentError.new("size (#{size}) can't be greater than buffer_size #{@buffer_size}")
    end

    to_read = @buffer_size - @in_buffer_rem.size
    return @in_buffer_rem[0, size] unless to_read.positive?

    in_buffer = in_buffer()

    while @in_buffer_rem.size < size
      target = Slice.new(in_buffer + @in_buffer_rem.size, to_read)
      read_size = unbuffered_read(target).to_i
      @in_buffer_rem = Slice.new(in_buffer, @in_buffer_rem.size + read_size)
      to_read -= read_size
    end

    @in_buffer_rem[0, size]
  end
end
