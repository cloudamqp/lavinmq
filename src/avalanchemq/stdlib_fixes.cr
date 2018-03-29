class Fiber
  def self.list
    fiber = @@first_fiber
    while fiber
      yield(fiber)
      fiber = fiber.next_fiber
    end
  end
end

abstract class Channel(T)
  def close
    @closed = true
    Scheduler.enqueue @senders
    @senders.clear
    Scheduler.enqueue @receivers
    @receivers.clear
    nil
  end
end

class Channel::Unbuffered(T) < Channel(T)
  def close
    super
    if sender = @sender
      Scheduler.enqueue sender
      @sender = nil
    end
  end
end
