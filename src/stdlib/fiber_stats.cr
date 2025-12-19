class Fiber
  # Expose stack usage for debugging
  # Stack grows downward: bottom is highest address, top is current position
  def stack_used : UInt64
    # When fiber is suspended, context.stack_top has the saved stack pointer
    # bottom - top = bytes used
    @stack.bottom.address - @context.stack_top.address
  end
end
