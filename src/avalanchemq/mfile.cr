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
# Max 2GB files (Slice is currently limited to Int32)
# If no `capacity` is given the file is open in read only mode and
# `capacity` is set to the current file size.
# If the process crashes the file will be `capacity` large,
# not `size` large, only on graceful close is the file truncated to its `size`.
# The file does not expand further than initial `capacity`, unless manually expanded.
class MFile < IO
  property pos = 0
  getter? closed = false
  getter size = 0
  getter capacity = 0
  getter path : String
  @buffer : Bytes

  # Map a file, if no capacity is given the file must exists and
  # the file will be mapped as readonly
  # The file won't be truncated if the capacity is smaller than current size
  def initialize(@path : String, capacity : Int? = nil)
    @readonly = capacity.nil?
    fd = open_fd
    @size = file_size(fd)
    @capacity = capacity ? Math.max(capacity.to_i32, @size) : @size
    truncate(fd, @capacity) if @capacity > @size
    @buffer = mmap(fd)
    close_fd(fd)
  end

  # Opens an existing file in readonly mode
  def self.open(path) : MFile
    self.new(path)
  end

  # Opens an existing file in readonly mode
  def self.open(path)
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

  private def file_size(fd) : Int32
    code = LibC.fstat(fd, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_size.to_i32
  end

  def disk_usage
    code = LibC.stat(@path.check_no_null_byte, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_blocks.to_u64 * 512
  end

  private def truncate(fd, size)
    code = LibC.ftruncate(fd, size)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
  end

  private def mmap(fd) : Bytes
    protection = LibC::PROT_READ
    protection |= LibC::PROT_WRITE # unless @readonly
    flags = LibC::MAP_SHARED
    buffer = LibC.mmap(nil, @capacity, protection, flags, fd, 0).as(UInt8*)
    raise RuntimeError.from_errno("mmap") if buffer == LibC::MAP_FAILED
    Bytes.new(buffer, @capacity, read_only: @readonly)
  end

  private def close_fd(fd)
    code = LibC.close(fd)
    raise File::Error.from_errno("Error closing file", file: @path) if code < 0
  end

  @deleted = false

  def delete
    code = LibC.unlink(@path.check_no_null_byte)
    raise File::Error.from_errno("Error deleting file", file: @path) if code < 0
    @deleted = true
  end

  # Unmapping the file
  # The file will be truncated to the current position unless readonly or deleted
  def close(truncate_to_size = true) : Nil
    return if @closed
    @closed = true
    code = LibC.munmap(@buffer, @capacity)
    raise RuntimeError.from_errno("Error unmapping file") if code == -1
    return if @readonly || @deleted || !truncate_to_size
    code = LibC.truncate(@path.check_no_null_byte, @size)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
  end

  def flush
    msync(@buffer.to_unsafe, @pos, LibC::MS_ASYNC)
  end

  def fsync
    msync(@buffer.to_unsafe, @pos, LibC::MS_SYNC)
  end

  private def msync(addr, len, flag) : Nil
    code = LibC.msync(addr, len, flag)
    raise RuntimeError.from_errno("msync") if code < 0
  end

  def finalize
    close(truncate_to_size: false)
  end

  def write(slice : Bytes) : Nil
    pos = @pos
    raise IO::EOFError.new if pos + slice.size > @capacity
    slice.copy_to(@buffer + pos)
    @pos = pos += slice.size
    @size = pos if pos > @size
  end

  def read(slice : Bytes)
    pos = @pos
    len = pos + slice.size
    raise IO::EOFError.new if len > @size
    (@buffer + pos).copy_to(slice.to_unsafe, slice.size)
    @pos = len
    slice.size
  end

  def rewind
    @pos = 0
  end

  def seek(offset, whence : IO::Seek = IO::Seek::Set)
    case whence
    in IO::Seek::Set
      @pos = offset
    in IO::Seek::Current
      @pos += offset
    in IO::Seek::End
      @pos = @size + offset
    end
  end

  def to_slice
    @buffer
  end

  def punch_hole(size : UInt32, offset : UInt32 = 0u32) : UInt32
    {% if flag?(:linux) %}
      return 0u32 if size < PAGESIZE
      o = offset

      # page align offset
      offset = page_align(offset)

      # adjust size accordingly
      size -= offset - o

      # page align size too, but downwards
      if size & PAGESIZE - 1 != 0
        size = page_align(size)
        size -= PAGESIZE
      end
      return 0u32 if size == 0

      addr = @buffer + offset
      if LibC.madvise(addr, size, LibC::MADV_REMOVE) != 0
        raise IO::Error.from_errno("madvise")
      end
      size
    {% else %}
      return 0u32 # only linux supports MADV_REMOVE/hole punching
    {% end %}
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

  def truncate(new_size : Int) : Int32
    new_size = page_align(new_size.to_i32)
    bytes = @capacity - new_size
    return 0 if bytes < PAGESIZE # don't bother truncating less than a page
    code = LibC.munmap(@buffer + new_size, bytes)
    raise RuntimeError.from_errno("Error unmapping file") if code == -1
    @capacity = @size = new_size
    code = LibC.truncate(@path.check_no_null_byte, new_size)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
    bytes
  end

  def resize(new_size : Int) : Nil
    capacity = new_size.to_i32

    # resize the file first
    code = LibC.truncate(@path.check_no_null_byte, capacity)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0

    {% if flag?(:linux) %}
      # then remap
      buffer = LibC.mremap(@buffer, @capacity, capacity.to_i32, LibC::MREMAP_MAYMOVE).as(UInt8*)
      raise RuntimeError.from_errno("mremap") if buffer == LibC::MAP_FAILED
      @capacity = capacity
      @buffer = Bytes.new(buffer, @capacity, read_only: @readonly)
    {% else %}
      # unmap and then mmap again
      code = LibC.munmap(@buffer, @capacity)
      raise RuntimeError.from_errno("Error unmapping file") if code == -1

      # mmap again
      fd = open_fd
      @capacity = capacity
      @buffer = mmap(fd)
      close_fd(fd)
    {% end %}
  end

  def move(new_path)
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
