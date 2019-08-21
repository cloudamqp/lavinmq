# https://github.com/crystal-lang/crystal/pull/8006
class Channel
  struct TimeoutAction
    include SelectAction

    @timeout_at : Time::Span

    def initialize(timeout : Time::Span)
      @timeout_at = Time.monotonic + timeout
    end

    def ready?
      Fiber.current.timed_out?
    end

    def execute : Nil
      Fiber.current.timed_out = false
    end

    def wait
      Fiber.timeout @timeout_at - Time.monotonic
    end

    def unwait
      Fiber.cancel_timeout
    end
  end
end

def timeout_select_action(timeout)
  Channel::TimeoutAction.new(timeout)
end
