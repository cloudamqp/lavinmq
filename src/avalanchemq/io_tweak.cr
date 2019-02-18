class IO
  def self.copy(src, dst, limit : Int)
    raise ArgumentError.new("Negative limit") if limit < 0

    buffer = uninitialized UInt8[131_072]
    remaining = limit
    while (len = src.read(buffer.to_slice[0, Math.min(buffer.size, Math.max(remaining, 0))])) > 0
      dst.write buffer.to_slice[0, len]
      remaining -= len
    end
    limit - remaining
  end
end
