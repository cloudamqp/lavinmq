# https://github.com/crystal-lang/crystal/pull/8004
class Channel::Buffered(T) < Channel(T)
  def send(value : T)
    while full?
      raise_if_closed
      @senders << Fiber.current
      Crystal::Scheduler.reschedule
    end

    raise_if_closed

    @queue << value
    if receiver = @receivers.shift?
      Crystal::Scheduler.enqueue receiver
    end

    self
  end

  private def receive_impl
    while empty?
      yield if @closed
      @receivers << Fiber.current
      Crystal::Scheduler.reschedule
    end

    @queue.shift.tap do
      if sender = @senders.shift?
        Crystal::Scheduler.enqueue sender
      end
    end
  end
end
