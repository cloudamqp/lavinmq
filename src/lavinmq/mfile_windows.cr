lib LibC
  fun close(fd : Int) : Int
  fun open(path : Char*, flags : Int, mode : Int) : Int
end

# Fallback for memory mapped files (on windows)
class MFile < IO
  class ClosedError < IO::Error
    def initialize
      super("File closed")
    end
  end

  getter pos : Int64 = 0i64
  getter size : Int64 = 0i64
  getter capacity : Int64 = 0i64
  getter path : String
  getter fd : Int32
  @buffer = Pointer(UInt8).null
  @deleted = false

  # Open a file, if no capacity is given the file must exists and
  # the file will be mapped as readonly
  # The file won't be truncated if the capacity is smaller than current size
  def initialize(@path : String, capacity : Int? = nil, @writeonly = false)
    @readonly = capacity.nil?
    raise ArgumentError.new("can't be both read only and write only") if @readonly && @writeonly
    @fd = open_fd
    @size = file_size
    @capacity = capacity ? Math.max(capacity.to_i64, @size) : @size
  end

  # Opens an existing file in readonly mode
  def self.open(path, & : self -> _)
    mfile = self.new(path)
    begin
      yield mfile
    ensure
      mfile.close
    end
  end

  def inspect(io : IO)
    io << "#<" << self.class << ": " << "@path=" << @path << " @size=" << @size << ">"
  end

  private def open_fd # TO-DO
    flags = @readonly ? LibC::O_RDONLY : LibC::O_CREAT | LibC::O_RDWR
    perms = 0o644 # TO-DO: Should be windows perms?
    fd = LibC.open(@path.check_no_null_byte, flags, perms)
    raise File::Error.from_errno("Error opening file", file: @path) if fd < 0
    fd
  end

  private def file_size : Int64
    return 0_i64 unless File.exists?(@path)
    File.size(@path).to_i64
  end

  def delete : self
    File.delete(@path.check_no_null_byte)
    self
  rescue e
    Log.warn { "Error deleting file #{@path}. Error: #{e.message}" }
    self
  end

  def close(truncate_to_size = true)
    LibC.CloseHandle(pointerof(@fd)) if @fd > -1
  end

  # Copies the file to another IO
  def copy_to(output : IO, size = @size) : Int64
    io = IO::FileDescriptor.new(@fd.to_u64, blocking: true, close_on_finalize: false)
    io.rewind
    IO.copy(io, output, size) == size || raise IO::EOFError.new
    size.to_i64
  end

  def finalize
    close
  end

  def write(slice : Bytes) : Nil
    size = @size
    new_size = size + slice.size
    raise IO::EOFError.new if new_size > @capacity
    File.write(@path, slice, mode: "a")
    @size = new_size
  end

  def write_bytes(slice : Bytes) : Nil
    write(slice)
  end

  def read(slice : Bytes)
    pos = @pos
    new_pos = pos + slice.size
    bytes = to_slice(pos, slice.size)
    @pos = new_pos
    slice.size
  end

  def rewind
    @pos = 0u64
  end

  def seek(offset : Int, whence : IO::Seek = IO::Seek::Set)
    case whence
    in IO::Seek::Set
      pos = offset.to_i64
    in IO::Seek::Current
      pos = @pos + offset
    in IO::Seek::End
      pos = @size + offset
    end
    raise ArgumentError.new("Can't seek ahead start of file") if pos.negative?
    @pos = pos
  end

  def pos=(pos)
    seek(pos, IO::Seek::Set)
  end

  def skip(bytes_count : Int) : Int
    pos = @pos + bytes_count
    if pos > @size
      @pos = @size
      raise IO::EOFError.new
    else
      @pos = pos
    end
    bytes_count
  end

  def to_slice(pos : Int64, size)
    raise IO::EOFError.new if pos + size > @size
    bytes = Bytes.new(size)

    File.open(@path) do |io|
      io.read_at(pos, size) { |b| b.read(bytes) }
    end
    bytes
  end

  def to_slice
    to_slice(0, @size)
  end

  def resize(new_size : Int) : Nil
  end

  def read_at(pos, bytes : Slice(UInt8))
    @pos = pos
    read(bytes)
    bytes
  end

  def read_at(pos, bytes_count : Int)
    @pos = pos
    bytes = Bytes.new(bytes_count)
    read(bytes)
    bytes
  end

  private def buffer : Pointer(UInt8) # no-op
    return Pointer(UInt8).null
  end

  def unmapped? : Bool
    false
  end

  def flush # no-op
  end

  def fsync : Nil # no-op
  end

  def unmap : Nil # no-op
  end
end
