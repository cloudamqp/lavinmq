class IO
  def self.copy(src, dst) : UInt64
    buffer = uninitialized UInt8[16384]
    count = 0_u64
    while (len = src.read(buffer.to_slice).to_i32) > 0
      dst.write buffer.to_slice[0, len]
      count += len
      Fiber.yield
    end
    count
  end

  def self.copy(src, dst, limit : Int) : UInt64
    return 0_u64 if limit.zero?
    raise ArgumentError.new("Negative limit") if limit < 0

    limit = limit.to_u64

    buffer = uninitialized UInt8[16384]
    remaining = limit
    while (len = src.read(buffer.to_slice[0, Math.min(buffer.size, Math.max(remaining, 0))])) > 0
      dst.write buffer.to_slice[0, len]
      remaining -= len
      break if remaining.zero?
      Fiber.yield
    end
    limit - remaining
  end

  def skip(bytes_count : Int) : Int
    remaining = bytes_count
    buffer = uninitialized UInt8[16384]
    while remaining > 0
      read_count = read(buffer.to_slice[0, Math.min(remaining, 16384)])
      raise IO::EOFError.new if read_count == 0
      remaining -= read_count
    end
    bytes_count
  end

  # Reads and discards bytes from `self` until there
  # are no more bytes.
  def skip_to_end : Int
    bytes_count = 0
    buffer = uninitialized UInt8[16384]
    while (len = read(buffer.to_slice)) > 0
      bytes_count += len
    end
    bytes_count
  end
end

module IO::Buffered
  def skip(bytes_count) : Int
    check_open

    if bytes_count <= @in_buffer_rem.size
      @in_buffer_rem += bytes_count
      return bytes_count
    end

    remaining = bytes_count
    remaining -= @in_buffer_rem.size
    @in_buffer_rem = Bytes.empty

    super(remaining)
    bytes_count
  end
end
