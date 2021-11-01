class IO::Memory
  def capacity
    @capacity
  end

  # Constructor that takes a Pointer
  def initialize(buffer : Pointer(UInt8), count : Int, writeable = true)
    @buffer = buffer
    @bytesize = @capacity = count.to_i
    @pos = 0
    @closed = false
    @resizeable = false
    @writeable = writeable
  end
end
