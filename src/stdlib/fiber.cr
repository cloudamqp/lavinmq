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

# Fix "FATAL: can't resume a running fiber"
class Fiber
  def resume : Nil
    Crystal::Scheduler.resume(self) unless self.running?
  end
end
