class Fiber
  def self.list
    fiber = @@first_fiber
    while fiber
      yield(fiber)
      fiber = fiber.next_fiber
    end
  end
end
