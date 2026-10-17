struct Slice(T)
  # Truncate to 72 first items
  def to_s(io : IO) : Nil
    if T == UInt8
      io << "Bytes["
      first(72).join io, ", ", &.to_s(io)
      io << ", ..." if size > 72
      io << ']'
    else
      io << "Slice["
      first(72).join io, ", ", &.inspect(io)
      io << ", ..." if size > 72
      io << ']'
    end
  end
end
