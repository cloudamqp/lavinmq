module Crystal::EventLoop
  def self.create_timeout_event(fiber)
    @@eb.new_event(-1, LibEvent2::EventFlags::None, fiber) do |_s, _flags, data|
      f = data.as(Fiber)
      f.timed_out = true
      f.resume
    end
  end
end
