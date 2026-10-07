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
  def stack_used : UInt64?
    if same?(Fiber.current)
      sp = uninitialized UInt8
      @stack.bottom.address - pointerof(sp).address
    elsif running?
      nil
    else
      @stack.bottom.address - @context.stack_top.address
    end
  end
end
