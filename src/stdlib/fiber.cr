class Fiber
  def self.list(&blk : Fiber -> Nil)
    fibers.unsafe_each &blk
  end

  def self.count
    c = 0
    Fiber.list { |_| c += 1 }
    c
  end

  # Approximate stack usage, for debugging. The saved stack pointer is only
  # updated on context switches, so for the current fiber the actual stack
  # pointer is used, and nil is returned for fibers running on other threads.
  # Wrapping subtraction: a fiber switching context on another thread can be
  # read half-updated, which must print a bogus value, not raise.
  def stack_used : UInt64?
    if same?(Fiber.current)
      sp = uninitialized UInt8
      @stack.bottom.address &- pointerof(sp).address
    elsif running?
      nil
    else
      @stack.bottom.address &- @context.stack_top.address
    end
  end

  # A dead fiber can stay reachable for a long time: a scheduler's run queue
  # doesn't clear the slot of a dequeued fiber (up to 256 per scheduler), and
  # a reused stack can keep a stale pointer to its previous fiber. Its proc's
  # closure would then keep whatever the fiber captured alive, e.g. a closed
  # connection's socket and client, so drop the proc once the fiber has
  # finished.
  # :nodoc:
  def self.inactive(fiber : Fiber)
    previous_def
    fiber.drop_proc
  end

  # :nodoc:
  protected def drop_proc : Nil
    @proc = -> { }
  end
end
