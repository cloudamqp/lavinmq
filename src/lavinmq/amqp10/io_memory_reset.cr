class IO::Memory
  # The only stdlib-class reopen in this codebase (elsewhere we read foreign
  # ivars, e.g. `obj.@ivar`, but never assign them -- that's a syntax error).
  # Lets FrameReader/ReceiverLink/Client reuse one IO::Memory across frames
  # instead of allocating a fresh one per frame/message, matching what
  # SliceReader#reset did. Mirrors IO::Memory.new(slice)'s own constructor.
  def reset(bytes : Bytes) : self
    @buffer = bytes.to_unsafe
    @bytesize = @capacity = bytes.bytesize
    @pos = 0
    @resizeable = false
    @writeable = !bytes.read_only?
    self
  end
end
