class Fiber
  def self.list(&blk : Fiber -> Nil)
    @@fibers.unsafe_each(&blk)
  end

  def wakeup
    raise "Can't wakeup one self" if self == Fiber.current
    @resume_event.try &.delete
    Crystal::Scheduler.enqueue(Fiber.current)
    Crystal::Scheduler.resume(self)
  end
end

require "openssl"
require "io"

abstract class OpenSSL::SSL::Socket
  def read_timeout=(read_timeout)
    io = @bio.io
    if io.responds_to? :read_timeout
      io.read_timeout = read_timeout
    else
      raise NotImplementedError.new("#{io.class}#read_timeout")
    end
  end

  def write_timeout=(write_timeout)
    io = @bio.io
    if io.responds_to? :write_timeout
      io.write_timeout = write_timeout
    else
      raise NotImplementedError.new("#{io.class}#write_timeout")
    end
  end
end

lib LibC
  {% if flag?(:linux) %}
    fun get_phys_pages : Int32
    fun getpagesize : Int32
  {% end %}

  {% if flag?(:darwin) %}
    SC_PAGESIZE = 29
    SC_PHYS_PAGES = 200
  {% end %}
end

module System
  def self.physical_memory
    {% if flag?(:linux) %}
      LibC.get_phys_pages * LibC.getpagesize
    {% elsif flag?(:darwin) %}
      LibC.sysconf(LibC::SC_PHYS_PAGES) * LibC.sysconf(LibC::SC_PAGESIZE)
    {% else %}
      raise NotImplementedError.new("System.physical_memory")
    {% end %}
  end
end

module IO::Buffered
  @buffer_size = 8192

  # Return the buffer size used
  def buffer_size
    @buffer_size
  end

  # Set the buffer size of both the read and write buffer
  # Cannot be changed after any of the buffers have been allocated
  def buffer_size=(value)
    if @in_buffer || @out_buffer
      raise ArgumentError.new("Cannot change buffer_size after buffers have been allocated")
    end
    @buffer_size = value
  end

  # Buffered implementation of `IO#read(slice)`.
  def read(slice : Bytes)
    check_open

    count = slice.size
    return 0 if count == 0

    if @in_buffer_rem.empty?
      # If we are asked to read more than half the buffer's size,
      # read directly into the slice, as it's not worth the extra
      # memory copy.
      if !read_buffering? || count >= @buffer_size // 2
        return unbuffered_read(slice).to_i
      else
        fill_buffer
        return 0 if @in_buffer_rem.empty?
      end
    end

    to_read = Math.min(count, @in_buffer_rem.size)
    slice.copy_from(@in_buffer_rem.to_unsafe, to_read)
    @in_buffer_rem += to_read
    to_read
  end

  # Buffered implementation of `IO#write(slice)`.
  def write(slice : Bytes)
    check_open

    return if slice.empty?

    count = slice.size

    if sync?
      return unbuffered_write(slice)
    end

    if count >= @buffer_size
      flush
      return unbuffered_write(slice)
    end

    if count > @buffer_size - @out_count
      flush
    end

    slice.copy_to(out_buffer + @out_count, count)
    @out_count += count
    nil
  end

  # :nodoc:
  def write_byte(byte : UInt8)
    check_open

    if sync?
      return super
    end

    if @out_count >= @buffer_size
      flush
    end
    out_buffer[@out_count] = byte
    @out_count += 1
  end

  private def fill_buffer
    in_buffer = in_buffer()
    size = unbuffered_read(Slice.new(in_buffer, @buffer_size)).to_i
    @in_buffer_rem = Slice.new(in_buffer, size)
  end

  private def in_buffer
    @in_buffer ||= GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
  end

  private def out_buffer
    @out_buffer ||= GC.malloc_atomic(@buffer_size.to_u32).as(UInt8*)
  end
end

class File
  def hint_target_size(size)
    {% if flag?(:linux) %}
      if LibC.fallocate(fd, LibC::FALLOC_FL_KEEP_SIZE, 0, size) != 0
        raise Errno.new("fallocate failed")
      end
    {% end %}
  end

  def advise(advice)
    {% if flag?(:linux) %}
      if LibC.posix_fadvise(fd, 0, 0, advice) != 0
        raise Errno.new("fadvise failed")
      end
    {% end %}
  end

  enum Advice
    Normal
    Random
    Sequential
    WillNeed
    DontNeed
    NoReuse
  end
end

lib LibC
  {% if flag?(:linux) %}
    fun fallocate(fd : Int, mode : Int, offset : OffT, len : OffT) : Int
    FALLOC_FL_KEEP_SIZE = 0x01
    FALLOC_FL_PUNCH_HOLE = 0x02
    FALLOC_FL_NO_HIDE_STALE = 0x04
    FALLOC_FL_COLLAPSE_RANGE = 0x08
    FALLOC_FL_ZERO_RANGE = 0x10
    FALLOC_FL_INSERT_RANGE = 0x20
    FALLOC_FL_UNSHARE_RANGE = 0x40

    fun posix_fadvise(fd : Int, offset : OffT, len : OffT, advice : Int) : Int
    POSIX_FADV_NORMAL     = 0 # No further special treatment.
    POSIX_FADV_RANDOM     = 1 # Expect random page references.
    POSIX_FADV_SEQUENTIAL = 2 # Expect sequential page references.
    POSIX_FADV_WILLNEED   = 3 # Will need these pages.
    POSIX_FADV_DONTNEED   = 4 # Don't need these pages.
    POSIX_FADV_NOREUSE    = 5 # Data will be accessed once.
  {% end %}
end
