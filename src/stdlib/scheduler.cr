class Crystal::Scheduler
  def self.timeout(time : Time::Span) : Nil
    Thread.current.scheduler.timeout(time)
  end

  def self.cancel_timeout : Nil
    Thread.current.scheduler.cancel_timeout
  end

  protected def timeout(time : Time::Span) : Nil
    @current.timeout_event.add(time)
  end

  protected def cancel_timeout : Nil
    @current.timeout_event.delete
  end
end
