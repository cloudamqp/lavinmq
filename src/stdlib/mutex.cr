class Mutex
  def assert_locked!
    if @mutex_fiber != Fiber.current
      raise "Mutex not locked by current fiber"
    end
  end
end
