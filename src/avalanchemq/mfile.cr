require "../stdlib/libc"
# Memory mapped file
class MFile < IO
  property pos = 0
  getter? closed = false
  getter capacity = 0
  @buffer : Bytes

  def initialize(@path : String, capacity)
    @capacity = capacity.to_i32
    fd = open_fd
    truncate(fd, @capacity)
    @buffer = mmap(fd)
    close_fd(fd)
  end

  # mmaps an existing file
  def self.open(path)
    if info = File.info? path
      self.new(path, info.size)
    else
      raise File::Error.new "File not found", file: path
    end
  end

  private def open_fd
    flags = LibC::O_CREAT | LibC::O_RDWR
    fd = LibC.open(@path.check_no_null_byte, flags, 0o644)
    if fd < 0
      raise ::File::Error.from_errno("Error opening file", file: @path)
    end
    fd
  end

  private def truncate(fd, size)
    code = LibC.ftruncate(fd, size)
    if code != 0
      raise ::File::Error.from_errno("Error truncating file", file: @path)
    end
  end

  private def mmap(fd) : Bytes
    protection = LibC::PROT_READ | LibC::PROT_WRITE
    flags = LibC::MAP_SHARED
    buffer = LibC.mmap(nil, @capacity, protection, flags, fd, 0).as(UInt8*)
    raise RuntimeError.from_errno("mmap") if buffer == LibC::MAP_FAILED
    buffer.to_slice(@capacity)
  end

  private def close_fd(fd)
    res = LibC.close(fd)
    raise ::File::Error.from_errno("Error closing file", file: @path) unless res.zero?
  end

  def delete
    err = LibC.unlink(@path.check_no_null_byte)
    if err == -1
      raise ::File::Error.from_errno("Error deleting file", file: path)
    end
  end

  def close : Nil
    return if @closed
    @closed = true
    res = LibC.munmap(@buffer, @capacity)
    raise RuntimeError.from_errno("munmap") if res == -1
    #res = LibC.truncate(@path.check_no_null_byte, @pos)
    #raise RuntimeError.from_errno("truncate") if res == -1
  end

  def closed?
    @closed
  end

  def flush
    msync(@buffer.to_unsafe, @pos, LibC::MS_ASYNC)
  end

  def fsync
    msync(@buffer.to_unsafe, @pos, LibC::MS_SYNC)
  end

  private def msync(addr, len, flag) : Nil
    res = LibC.msync(addr, len, flag)
    raise RuntimeError.from_errno("msync") if res == -1
  end

  def finalize
    close
  end

  def write(slice : Bytes) : Nil
    raise IO::Error.new("Out of capacity") if @capacity - @pos < slice.size
    slice.copy_to(@buffer + @pos)
    @pos += slice.size
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
