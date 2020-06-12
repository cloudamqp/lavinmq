lib LibC
  MS_ASYNC = 0x0001
  MS_SYNC = 0x0004
  fun munmap(addr : Void*, len : SizeT) : Int
  fun msync(addr : Void*, len : SizeT, flags : Int) : Int
  fun truncate(path : Char*, len : OffT) : Int
end

# Memory mapped file
# Max 2GB files (Slice is currently limited to Int32)
# Beware, the file will be truncated to the position you're at when closing
class MFile < IO
  property pos = 0
  getter? closed = false
  getter capacity = 0
  @buffer : Bytes

  def initialize(@path : String, capacity : Int, @readonly = false)
    @capacity = capacity.to_i32
    fd = open_fd
    truncate(fd, @capacity) unless @readonly
    @buffer = mmap(fd)
    close_fd(fd)
  end

  # mmaps an existing file
  def self.open(path)
    if info = File.info? path
      self.new(path, info.size, readonly: true)
    else
      raise File::Error.new("File not found", file: path)
    end
  end

  private def open_fd
    flags = LibC::O_CREAT | LibC::O_RDWR
    fd = LibC.open(@path.check_no_null_byte, flags, 0o644)
    raise File::Error.from_errno("Error opening file", file: @path) if fd < 0
    fd
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
    code = LibC.truncate(@path.check_no_null_byte, @pos)
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
      @pos = @capacity + offset
    end
  end
end
