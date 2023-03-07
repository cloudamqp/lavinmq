lib LibC
  MS_ASYNC       = 0x0001
  MREMAP_MAYMOVE =      1
  {% if flag?(:linux) %}
    MS_SYNC     = 0x0004
    MADV_REMOVE =      9
  {% else %}
    MS_SYNC = 0x0010
  {% end %}
  fun munmap(addr : Void*, len : SizeT) : Int
  fun mremap(addr : Void*, old_len : SizeT, new_len : SizeT, flags : Int) : Void*
  fun msync(addr : Void*, len : SizeT, flags : Int) : Int
  fun truncate(path : Char*, len : OffT) : Int
end

# Memory mapped file
# If no `capacity` is given the file is open in read only mode and
# `capacity` is set to the current file size.
# If the process crashes the file will be `capacity` large,
# not `size` large, only on graceful close is the file truncated to its `size`.
# The file does not expand further than initial `capacity`, unless manually expanded.
class MFile < IO
  getter pos : Int64 = 0i64
  getter? closed : Bool = false
  getter size : Int64 = 0i64
  getter capacity : Int64 = 0i64
  getter path : String
  @buffer : Pointer(UInt8)

  # Map a file, if no capacity is given the file must exists and
  # the file will be mapped as readonly
  # The file won't be truncated if the capacity is smaller than current size
  def initialize(@path : String, capacity : Int? = nil)
    @readonly = capacity.nil?
    @fd = open_fd
    @size = file_size
    @capacity = capacity ? Math.max(capacity.to_i64, @size) : @size
    if @capacity > @size
      code = LibC.ftruncate(@fd, @capacity)
      raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
    end
    @buffer = mmap
  end

  # Opens an existing file in readonly mode
  def self.open(path) : self
    self.new(path)
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

  private def open_fd
    flags = @readonly ? LibC::O_RDWR : LibC::O_CREAT | LibC::O_RDWR
    perms = 0o644
    fd = LibC.open(@path.check_no_null_byte, flags, perms)
    raise File::Error.from_errno("Error opening file", file: @path) if fd < 0
    fd
  end

  private def file_size : Int64
    code = LibC.fstat(@fd, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_size.to_i64
  end

  def disk_usage : Int64
    code = LibC.fstat(@fd, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_blocks.to_i64 * 512
  end

  private def mmap : Pointer(UInt8)
    protection = LibC::PROT_READ
    protection |= LibC::PROT_WRITE # unless @readonly
    flags = LibC::MAP_SHARED
    ptr = LibC.mmap(nil, @capacity, protection, flags, @fd, 0)
    raise RuntimeError.from_errno("mmap") if ptr == LibC::MAP_FAILED
    ptr.as(UInt8*)
  end

  @deleted = false

  def delete : self
    code = LibC.unlink(@path.check_no_null_byte)
    raise File::Error.from_errno("Error deleting file", file: @path) if code < 0
    @deleted = true
    self
  end

  # Unmapping the file
  # The file will be truncated to the current position unless readonly or deleted
  def close(truncate_to_size = true)
    return if @closed
    @closed = true
    begin
      munmap

      return if @readonly || @deleted || !truncate_to_size
      code = LibC.ftruncate(@fd, @size)
      raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
    ensure
      code = LibC.close(@fd)
      raise File::Error.from_errno("Error closing file", file: @path) if code < 0
    end
  end

  def flush
    msync(@buffer, @pos, LibC::MS_ASYNC)
  end

  def fsync
    msync(@buffer, @pos, LibC::MS_SYNC)
  end

  private def munmap(buffer = @buffer, length = @capacity)
    code = LibC.munmap(buffer, length)
    raise RuntimeError.from_errno("Error unmapping file") if code == -1
  end

  private def msync(addr, len, flag) : Nil
    code = LibC.msync(addr, len, flag)
    raise RuntimeError.from_errno("msync") if code < 0
  end

  def finalize
    close(truncate_to_size: false)
  end

  def write(slice : Bytes) : Nil
    raise IO::Error.new("MFile closed") if @closed
    pos = @pos
    raise IO::EOFError.new if pos + slice.size > @capacity
    slice.copy_to(@buffer + pos, slice.size)
    @pos = pos += slice.size
    @size = pos.to_i64 if pos > @size
  end

  def read(slice : Bytes)
    raise IO::Error.new("MFile closed") if @closed
    pos = @pos
    len = pos + slice.size
    raise IO::EOFError.new if len > @size
    (@buffer + pos).copy_to(slice.to_unsafe, slice.size)
    @pos = len
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
    raise ArgumentError.new("Can't seek beyond end of file") if pos > @size
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

  def to_unsafe
    @buffer
  end

  def to_slice
    Bytes.new(@buffer, @size, read_only: true)
  end

  def to_slice(pos, size)
    raise IO::EOFError.new if pos + size > @size
    Bytes.new(@buffer + pos, size, read_only: true)
  end

  def advise(advice : Advice, offset = 0, length = @capacity)
    if LibC.madvise(@buffer + offset, length, advice) != 0
      raise IO::Error.from_errno("madvise")
    end
  end

  enum Advice
    Normal
    Random
    Sequential
    WillNeed
    DontNeed
  end

  def truncate(new_size : Int) : Nil
    return if new_size == @capacity
    code = LibC.ftruncate(@fd, new_size.to_i64)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
    old_capacity = @capacity
    @capacity = @size = new_size.to_i64

    offset = page_align(new_size.to_i64)
    length = old_capacity - offset
    munmap(@buffer + offset, length) if length > 0
  end

  def resize(new_size : Int) : Nil
    raise ArgumentError.new("Can't expand file larger than capacity, use truncate") if new_size > @capacity
    @size = new_size.to_i64
  end

  def rename(new_path)
    File.rename @path, new_path
    @path = new_path
  end

  PAGESIZE = LibC.sysconf(LibC::SC_PAGESIZE).to_u32

  private def page_align(n : Int) : Int
    n += PAGESIZE - 1
    n -= n & PAGESIZE - 1
    n
  end
end
