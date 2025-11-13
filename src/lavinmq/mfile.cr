require "wait_group"

lib LibC
  MS_ASYNC       = 1
  MREMAP_MAYMOVE = 1
  {% if flag?(:linux) %}
    MS_SYNC = 0x0004
    fun mremap(addr : Void*, old_len : SizeT, new_len : SizeT, flags : Int) : Void*
  {% else %}
    MS_SYNC = 0x0010
  {% end %}
  fun munmap(addr : Void*, len : SizeT) : Int
  fun msync(addr : Void*, len : SizeT, flags : Int) : Int
  fun truncate(path : Char*, length : OffT) : Int
end

# Memory mapped file
# If no `capacity` is given the file is open in read only mode and
# `capacity` is set to the current file size.
# If the process crashes the file will be `capacity` large,
# not `size` large, only on graceful close is the file truncated to its `size`.
# The file does not expand further than initial `capacity`, unless manually expanded.
class MFile < IO
  private PAGE_SIZE = LibC.sysconf(LibC::SC_PAGESIZE)

  getter pos : Int64 = 0i64
  getter size : Int64 = 0i64
  getter capacity : Int64 = 0i64
  getter path : String
  @buffer : Pointer(UInt8)
  @deleted = Atomic(Bool).new(false)
  @closed = Atomic(Bool).new(false)

  def closed?
    @closed.get(:acquire)
  end

  # Map a file, if no capacity is given the file must exists and
  # the file will be mapped as readonly
  # The file won't be truncated if the capacity is smaller than current size
  def initialize(@path : String, capacity : Int? = nil, @writeonly = false)
    @readonly = capacity.nil?
    raise ArgumentError.new("can't be both read only and write only") if @readonly && @writeonly
    fd = open_fd
    begin
      @size = file_size(fd)
      @capacity = capacity ? Math.max(capacity.to_i64, @size) : @size
      if @capacity > @size
        code = LibC.ftruncate(fd, @capacity)
        raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
      end
      @buffer = mmap(fd, @capacity)
    ensure
      # Close FD after mmap, we don't need it anymore
      close_fd(fd)
    end
  end

  def self.open(path, capacity : Int? = nil, writeonly = false, & : self -> _)
    mfile = self.new(path, capacity, writeonly)
    begin
      yield mfile
    ensure
      mfile.close
    end
  end

  def inspect(io : IO)
    io << "#<" << self.class << ": " << "@path=" << @path << " @size=" << @size << ">"
  end

  private def open_fd
    flags = @readonly ? LibC::O_RDONLY : LibC::O_CREAT | LibC::O_RDWR
    perms = 0o644
    fd = LibC.open(@path.check_no_null_byte, flags, perms)
    raise File::Error.from_errno("Error opening file", file: @path) if fd < 0
    fd
  end

  private def close_fd(fd)
    code = LibC.close(fd)
    raise File::Error.from_errno("Error closing file", file: @path) if code < 0
  end

  private def file_size(fd) : Int64
    code = LibC.fstat(fd, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_size.to_i64
  end

  private def mmap(fd, length = @capacity) : Pointer(UInt8)
    protection = case
                 when @readonly  then LibC::PROT_READ
                 when @writeonly then LibC::PROT_WRITE
                 else                 LibC::PROT_READ | LibC::PROT_WRITE
                 end
    flags = LibC::MAP_SHARED
    ptr = LibC.mmap(nil, length, protection, flags, fd, 0)
    raise RuntimeError.from_errno("mmap") if ptr == LibC::MAP_FAILED
    addr = ptr.as(UInt8*)
    advise(Advice::DontDump, addr, length)
    addr
  end

  def delete(*, raise_on_missing = true) : Nil
    return if @deleted.swap(true, :acquire_release) # avoid double deletes
    if raise_on_missing
      File.delete(@path)
    else
      File.delete?(@path)
    end
  end

  # The file will be truncated to the current position unless readonly or deleted
  def close(truncate_to_size = true)
    return if @closed.swap(true, :acquire_release)
    code = LibC.munmap(@buffer, @capacity)
    raise RuntimeError.from_errno("Error unmapping file") if code == -1
    if truncate_to_size && !@readonly && !@deleted.get(:acquire)
      code = LibC.truncate(@path.check_no_null_byte, @size)
      # Ignore ENOENT - file may have been deleted by another process
      if code < 0 && Errno.value != Errno::ENOENT
        raise File::Error.from_errno("Error truncating file", file: @path)
      end
    end
  end

  # Truncate the file to the given capacity (contracting only, no expansion)
  # The truncated part is unmapped from memory
  def truncate(new_capacity) : Nil
    new_capacity = new_capacity.to_i64
    old_capacity = @capacity
    return if new_capacity == old_capacity # no change
    raise ArgumentError.new("Cannot expand a MFile") if new_capacity > old_capacity

    # First truncate the file on disk
    code = LibC.truncate(@path.check_no_null_byte, new_capacity)
    raise File::Error.from_errno("Error truncating file", file: @path) if code < 0

    # Unmap the truncated part from the mapping
    unmap_truncated(new_capacity, old_capacity)

    @capacity = new_capacity
    @size = new_capacity if @size > new_capacity
    @pos = new_capacity if @pos > new_capacity
  end

  private def unmap_truncated(new_capacity, old_capacity) : Nil
    {% if flag?(:linux) %}
      # Use mremap on Linux for efficient in-place resizing
      ptr = LibC.mremap(@buffer, old_capacity, new_capacity, 0)
      raise RuntimeError.from_errno("mremap") if ptr == LibC::MAP_FAILED
      raise RuntimeError.new("mremap returned different address") if ptr.address != @buffer.address
    {% else %}
      # unmap requires a page-aligned address
      aligned_capacity = ((new_capacity + PAGE_SIZE - 1) // PAGE_SIZE) * PAGE_SIZE
      if aligned_capacity < old_capacity
        unmap_size = old_capacity - aligned_capacity
        code = LibC.munmap(@buffer + aligned_capacity, unmap_size)
        raise RuntimeError.from_errno("Error unmapping truncated region") if code == -1
      end
    {% end %}
  end

  def flush
    msync(@buffer, @size, LibC::MS_ASYNC)
  end

  def msync
    msync(@buffer, @size, LibC::MS_SYNC)
  end

  private def msync(addr, len, flag) : Nil
    return if len.zero?
    check_open
    code = LibC.msync(addr, len, flag)
    raise RuntimeError.from_errno("msync") if code < 0
  end

  # Append only
  def write(slice : Bytes) : Nil
    check_open
    size = @size
    new_size = size + slice.size
    raise IO::EOFError.new if new_size > @capacity
    slice.copy_to(@buffer + size, slice.size)
    @size = new_size
  end

  def read(slice : Bytes)
    check_open
    pos = @pos
    len = Math.min(slice.size, @size - pos)
    slice.copy_from(@buffer + pos, len)
    @pos = pos + len
    len
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

  def to_slice
    check_open
    Bytes.new(@buffer, @size, read_only: true)
  end

  def to_slice(pos, size)
    check_open
    raise IO::EOFError.new if pos + size > @size
    Bytes.new(@buffer + pos, size, read_only: true)
  end

  def advise(advice : Advice, addr = @buffer, length = @capacity) : Nil
    check_open
    if LibC.madvise(addr, length, advice) != 0
      raise IO::Error.from_errno("madvise, addr=#{addr} length=#{length} advice=#{advice.value}")
    end
  end

  enum Advice : LibC::Int
    Normal     = 0
    Random     = 1
    Sequential = 2
    WillNeed   = 3
    DontNeed   = 4
    {% if flag?(:linux) %}
      DontDump = 16
    {% else %}
      DontDump = 8 # is called NoCore in BSD/Darwin
    {% end %}
  end

  def dontneed
    advise(Advice::DontNeed)
  end

  # Resize the file, so that read operations can't happen beyond `new_size`
  # this does not change the actual file size on disk
  # and you can still write beyond `new_size`
  def resize(new_size : Int) : Nil
    raise ArgumentError.new("Can't expand file larger than capacity, use truncate") if new_size > @capacity
    @size = new_size.to_i64
    @pos = new_size.to_i64 if @pos > new_size
  end

  # Read from a specific position in the file
  def read_at(pos, slice)
    check_open
    len = Math.min(slice.size, @size - pos)
    slice.copy_from(@buffer + pos, len)
    len
  end

  def rename(new_path : String) : Nil
    File.rename @path, new_path
    @path = new_path
  end

  private def check_open
    raise IO::Error.new "Closed mfile" if closed?
  end
end
