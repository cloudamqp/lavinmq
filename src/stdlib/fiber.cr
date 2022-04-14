class Fiber
  def self.list(&blk : Fiber -> Nil)
    fibers.unsafe_each &blk
  end

  def self.count
    c = 0
    Fiber.list { |_| c += 1 }
    c
  end
end
