class Fiber
  def self.list(&blk : Fiber -> Nil)
    fibers.unsafe_each &blk
  end

  def self.count
    c = 0
    Fiber.list { |_| c += 1 }
    c
  end

  # Approximate stack usage, for debugging. Only accurate for suspended fibers,
  # as the saved stack pointer is updated on context switch.
  def stack_used : UInt64
    @stack.bottom.address - @context.stack_top.address
  end
end
