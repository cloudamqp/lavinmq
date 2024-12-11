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
end

# Memory mapped file
# If no `capacity` is given the file is open in read only mode and
# `capacity` is set to the current file size.
# If the process crashes the file will be `capacity` large,
# not `size` large, only on graceful close is the file truncated to its `size`.
# The file does not expand further than initial `capacity`, unless manually expanded.
class MFile < IO
  class ClosedError < IO::Error
    def initialize
      super("MFile closed")
    end
  end

  getter pos : Int64 = 0i64
  getter size : Int64 = 0i64
  getter capacity : Int64 = 0i64
  getter path : String
  getter fd : Int32
  @buffer = Pointer(UInt8).null
  @deleted = false
  @wg = WaitGroup.new

  # Map a file, if no capacity is given the file must exists and
  # the file will be mapped as readonly
  # The file won't be truncated if the capacity is smaller than current size
  def initialize(@path : String, capacity : Int? = nil, @writeonly = false)
    @readonly = capacity.nil?
    raise ArgumentError.new("can't be both read only and write only") if @readonly && @writeonly
    @fd = open_fd
    @size = file_size
    @capacity = capacity ? Math.max(capacity.to_i64, @size) : @size
    if @capacity > @size
      code = LibC.ftruncate(@fd, @capacity)
      raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
    end
  end

  # Opens an existing file in readonly mode
  def self.open(path, capacity : Int? = nil, & : self -> _)
    mfile = self.new(path, capacity)
    begin
      mfile.borrow
      yield mfile
    ensure
      mfile.unborrow
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

  private def file_size : Int64
    code = LibC.fstat(@fd, out stat)
    raise File::Error.from_errno("Unable to get info", file: @path) if code < 0
    stat.st_size.to_i64
  end

  private def mmap(length = @capacity) : Pointer(UInt8)
    return Pointer(UInt8).null if length.zero?
    protection = case
                 when @readonly  then LibC::PROT_READ
                 when @writeonly then LibC::PROT_WRITE
                 else                 LibC::PROT_READ | LibC::PROT_WRITE
                 end
    flags = LibC::MAP_SHARED
    ptr = LibC.mmap(nil, length, protection, flags, @fd, 0)
    raise RuntimeError.from_errno("mmap") if ptr == LibC::MAP_FAILED
    addr = ptr.as(UInt8*)
    advise(MFile::Advice::Sequential, addr, 0, length) unless @writeonly
    @buffer = addr
  end

  def delete(*, raise_on_missing = true) : self
    if raise_on_missing
      File.delete(@path)
    else
      File.delete?(@path)
    end
    @deleted = true
    self
  end

  @counter = 0
  @counter_lock = Mutex.new(:reentrant)

  def borrow : self
    @counter_lock.synchronize do
      counter = @counter
      if counter.zero?
        STDERR.puts "mmap #{@path}"
        mmap
      end
      @counter = counter + 1
    end
    self
  end

  def unborrow : self
    @counter_lock.synchronize do
      counter = @counter -= 1
      if counter.zero?
        STDERR.puts "unmap #{@path}"
        unmap
      end
    end
    self
  end

  def borrow(& : self -> _)
    borrow
    yield self
  ensure
    unborrow
  end

  # The file will be truncated to the current position unless readonly or deleted
  def close(truncate_to_size = true)
    if truncate_to_size && !@readonly && !@deleted && @fd > 0
      code = LibC.ftruncate(@fd, @size)
      raise File::Error.from_errno("Error truncating file", file: @path) if code < 0
    end
  ensure
    unless @fd == -1
      code = LibC.close(@fd)
      raise File::Error.from_errno("Error closing file", file: @path) if code < 0
      @fd = -1
    end
  end

  def flush
    msync(@buffer, @size, LibC::MS_ASYNC)
  end

  def msync
    msync(@buffer, @size, LibC::MS_SYNC)
  end

  def fsync : Nil
    ret = LibC.fsync(@fd)
    raise IO::Error.from_errno("Error syncing file") if ret != 0
  end

  # unload the memory mapping, will be remapped on demand
  private def unmap : Nil
    b = @buffer
    c = @capacity
    @buffer = Pointer(UInt8).null
    munmap(b, c)
  end

  private def munmap(buffer = @buffer, length = @capacity)
    return if length.zero? || buffer.null?
    code = LibC.munmap(buffer, length)
    raise RuntimeError.from_errno("Error unmapping file") if code == -1
  end

  private def msync(addr, len, flag) : Nil
    return if len.zero?
    code = LibC.msync(addr, len, flag)
    raise RuntimeError.from_errno("msync") if code < 0
  end

  def write(slice : Bytes) : Nil
    # raise IO::Error.new("not memory mapped") if @buffer.null?
    size = @size
    new_size = size + slice.size
    raise IO::EOFError.new if new_size > @capacity
    slice.copy_to(@buffer + size, slice.size)
    @size = new_size
  end

  def read(slice : Bytes)
    # raise IO::Error.new("not memory mapped") if @buffer.null?
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

  def advise(advice : Advice, addr = @buffer, offset = 0, length = @capacity) : Nil
    if LibC.madvise(addr + offset, length, advice) != 0
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

  def resize(new_size : Int) : Nil
    raise ArgumentError.new("Can't expand file larger than capacity, use truncate") if new_size > @capacity
    @size = new_size.to_i64
    @pos = new_size.to_i64 if @pos > new_size
  end

  # Read from a specific position in the file
  # but without mapping the whole file, it uses `pread`
  def read_at(pos, bytes)
    cnt = LibC.pread(@fd, bytes, bytes.bytesize, pos)
    raise IO::Error.from_errno("pread") if cnt == -1
    cnt
  end
end
