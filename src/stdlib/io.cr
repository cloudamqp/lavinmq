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
end
