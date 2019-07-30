class Fiber
  def self.list(&blk : Fiber -> Nil)
    @@fibers.unsafe_each(&blk)
  end
end

# https://github.com/crystal-lang/crystal/pull/7998
class Fiber
  def wakeup
    raise "Can't wakeup dead fibers" if dead?
    raise "Can't wakeup one self" if self == Fiber.current
    @resume_event.try &.delete
    Crystal::Scheduler.enqueue(Fiber.current)
    Crystal::Scheduler.resume(self)
  end
end

struct Crystal::Event
  def delete
    unless LibEvent2.event_del(@event) == 0
      raise "Error deleting event"
    end
  end
end

require "openssl"
require "io"

# https://github.com/crystal-lang/crystal/pull/7820
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

# No PR yet
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

# https://github.com/crystal-lang/crystal/pull/7930
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

# No PR yet
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

# https://github.com/crystal-lang/crystal/pull/8004
class Channel::Buffered(T) < Channel(T)
  def send(value : T)
    while full?
      raise_if_closed
      @senders << Fiber.current
      Crystal::Scheduler.reschedule
    end

    raise_if_closed

    @queue << value
    if receiver = @receivers.shift?
      Crystal::Scheduler.enqueue receiver
    end

    self
  end

  private def receive_impl
    while empty?
      yield if @closed
      @receivers << Fiber.current
      Crystal::Scheduler.reschedule
    end

    @queue.shift.tap do
      if sender = @senders.shift?
        Crystal::Scheduler.enqueue sender
      end
    end
  end
end

# https://github.com/crystal-lang/crystal/pull/8006
class Channel
  struct TimeoutAction
    include SelectAction

    def initialize(timeout : Time::Span)
      @timeout_at = Time.monotonic + timeout
    end

    def ready?
      Fiber.current.timed_out?
    end

    def execute : Nil
      Fiber.current.timed_out = false
    end

    def wait
      Fiber.timeout @timeout_at - Time.monotonic
    end

    def unwait
      Fiber.cancel_timeout
    end
  end
end

def timeout_select_action(timeout)
  Channel::TimeoutAction.new(timeout)
end

struct Crystal::Event
  def delete
    unless LibEvent2.event_del(@event) == 0
      raise "Error deleting event"
    end
  end
end

module Crystal::EventLoop
  def self.create_timeout_event(fiber)
    @@eb.new_event(-1, LibEvent2::EventFlags::None, fiber) do |s, flags, data|
      f = data.as(Fiber)
      f.timed_out = true
      f.resume
    end
  end
end

class Crystal::Scheduler
  def self.timeout(time : Time::Span) : Nil
    Thread.current.scheduler.timeout(time)
  end

  def self.cancel_timeout : Nil
    Thread.current.scheduler.cancel_timeout
  end

  protected def timeout(time : Time::Span) : Nil
    @current.timeout_event.add(time)
  end

  protected def cancel_timeout : Nil
    @current.timeout_event.delete
  end
end

class Fiber
  @timeout_event : Crystal::Event?
  property? timed_out = false

  # :nodoc:
  def timeout_event
    @timeout_event ||= Crystal::EventLoop.create_timeout_event(self)
  end

  # The current fiber will resume after a period of time
  # and have the property `timed_out` set to true.
  # The timeout can be cancelled with `cancel_timeout`
  def self.timeout(timeout : Time::Span?) : Nil
    Crystal::Scheduler.timeout(timeout)
  end

  def self.cancel_timeout
    Crystal::Scheduler.cancel_timeout
  end

  def run
    GC.unlock_read
    @proc.call
  rescue ex
    if name = @name
      STDERR.print "Unhandled exception in spawn(name: #{name}): "
    else
      STDERR.print "Unhandled exception in spawn: "
    end
    ex.inspect_with_backtrace(STDERR)
    STDERR.flush
  ensure
    Fiber.stack_pool.release(@stack)
    # Remove the current fiber from the linked list
    @@fibers.delete(self)

    # Delete the resume event if it was used by `yield` or `sleep`
    @resume_event.try &.free
    @timeout_event.try &.free

    @alive = false
    Crystal::Scheduler.reschedule
  end
end
