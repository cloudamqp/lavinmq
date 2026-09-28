require "./spec_helper"

describe "Fiber::ExecutionContext monitor" do
  {% if (!flag?(:without_mt) && !flag?(:preview_mt) || flag?(:execution_context)) &&
          compare_versions(Crystal::VERSION, "1.21.0") >= 0 &&
          compare_versions(Crystal::VERSION, "1.22.0-dev") < 0 %}
    it "backs off while idle" do
      should_eventually(be_true, 5.seconds) do
        Fiber::ExecutionContext.monitor_interval.not_nil! > 100.milliseconds
      end
    end

    it "returns to the default interval when a thread enters a blocking syscall" do
      should_eventually(be_true, 5.seconds) do
        Fiber::ExecutionContext.monitor_interval.not_nil! > 100.milliseconds
      end
      # File.open goes through Fiber.syscall (open(2))
      File.open(__FILE__) { }
      should_eventually(be_true, 100.milliseconds) do
        Fiber::ExecutionContext.monitor_interval.not_nil! <= 10.milliseconds
      end
    end

    it "hands off a scheduler blocked in a syscall while backed off" do
      ctx = Fiber::ExecutionContext::Parallel.new("monitor-spec", 1)
      should_eventually(be_true, 5.seconds) do
        Fiber::ExecutionContext.monitor_interval.not_nil! >= Fiber::ExecutionContext::Monitor::MAX_EVERY
      end
      done = Channel(Time::Span).new
      ctx.spawn do
        # blocks the context's only thread (not just the fiber)
        Fiber.syscall { Thread.sleep(1.second) }
      end
      sleep 10.milliseconds
      start = Time.instant
      ctx.spawn { done.send(Time.instant - start) }
      # without the wake-up, the backed-off monitor only notices the blocked
      # scheduler at its next tick, up to 1s later
      done.receive.should be < 200.milliseconds
    end
  {% else %}
    pending "not patched on Crystal #{Crystal::VERSION}"
  {% end %}
end
