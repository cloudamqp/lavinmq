lib LibC
  MS_ASYNC = 0x0001
  MS_SYNC = 0x0004
  fun munmap(addr : Void*, len : SizeT) : Int
  fun msync(addr : Void*, len : SizeT, flags : Int) : Int
  fun truncate(path : Char*, len : OffT) : Int
end

# Memory mapped file
# Max 2GB files (Slice is currently limited to Int32)
# Beware the file is truncated to its `capacity`, at graceful
# close the file will be trunacted to `@size` which hte longest we've
# written. But if there's a crash the file might be `@capacity` long,
# and the end will just be 0's.
class MFile < IO
  property pos = 0
  getter? closed = false
  getter size = 0
  getter capacity = 0
  @buffer : Bytes

  # Map a file, if no capacity is given the file must exists and
  # the file will be mapped as readonly
  def initialize(@path : String, capacity : Int? = nil)
    @readonly = capacity.nil?
    fd = open_fd
    @size = file_size(fd)
    @capacity = (capacity || @size).to_i32
    truncate(fd, @capacity) unless @readonly
    @buffer = mmap(fd)
    close_fd(fd)
  end

  # Opens an existing file in readonly mode
  def self.open(path)
    self.new(path)
  end

  private def open_fd
    flags = @readonly ? LibC::O_RDONLY : LibC::O_CREAT | LibC::O_RDWR
    perms = 0o644
    fd = LibC.open(@path.check_no_null_byte, flags, perms)
    raise File::Error.from_errno("Error opening file", file: @path) if fd < 0
    fd
  end

  private def file_size(fd)
    code = LibC.fstat(fd, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_size.to_i32
  end

  private def truncate(fd, size)
    code = LibC.ftruncate(fd, size)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
  end

  private def mmap(fd) : Bytes
    protection = LibC::PROT_READ
    protection |= LibC::PROT_WRITE unless @readonly
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
  def close : Nil
    return if @closed
    @closed = true
    code = LibC.munmap(@buffer, @capacity)
    raise RuntimeError.from_errno("Error unmapping file") if code == -1
    return if @readonly || @deleted
    code = LibC.truncate(@path.check_no_null_byte, @size)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
  end

  @synced_pos = 0

  def flush(from = @synced_pos, to = @pos)
    async(from, to)
  end

  def fsync(from = @synced_pos, to = @pos)
    sync(from, to)
  end

  def async(from = @synced_pos, to = @pos)
    addr = @buffer + from
    len = to - from
    msync(addr.to_unsafe, len, LibC::MS_ASYNC)
  end

  def sync(from = @synced_pos, to = @pos)
    addr = @buffer + from
    len = to - from
    msync(addr.to_unsafe, len, LibC::MS_SYNC)
    @synced_pos = @pos
  end

  private def msync(addr, len, flag) : Nil
    code = LibC.msync(addr, len, flag)
    raise RuntimeError.from_errno("msync") if code < 0
  end

  def finalize
    close
  end

  def write(slice : Bytes) : Int64
    raise IO::Error.new("Out of capacity") if @capacity - @pos < slice.size
    slice.copy_to(@buffer + @pos)
    @pos += slice.size
    @size = @pos if @pos > @size
    slice.size.to_i64
  end

  def read(slice : Bytes)
    (@buffer + @pos).copy_to(slice.to_unsafe, slice.size)
    @pos += slice.size
    slice.size
  end

  def rewind
    @pos = 0
  end

  def seek(offset, whence : IO::Seek = IO::Seek::Set)
    case whence
    when IO::Seek::Set
      @pos = offset
    when IO::Seek::Current
      @pos += offset
    when IO::Seek::End
      @pos = @size + offset
    end
  end
end
