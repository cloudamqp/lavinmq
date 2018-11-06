class Fiber
  def self.list(&blk : Fiber -> Nil)
    @@fibers.unsafe_each(&blk)
  end
end
