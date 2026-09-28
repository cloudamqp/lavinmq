require "./spec_helper"

{% if (!flag?(:without_mt) && !flag?(:preview_mt) || flag?(:execution_context)) &&
        compare_versions(Crystal::VERSION, "1.21.0") >= 0 &&
        compare_versions(Crystal::VERSION, "1.22.0-dev") < 0 %}
  # The monitor only backs off when the whole process is idle. Other specs can
  # leave background fibers running (e.g. reconnect loops), so skip the
  # integration examples if the process never gets there.
  private def wait_for_monitor_back_off(min : Time::Span)
    deadline = Time.instant + 5.seconds
    until Fiber::ExecutionContext.monitor_interval.not_nil! >= min
      pending!("process never went idle") if Time.instant > deadline
      sleep 10.milliseconds
    end
  end

  describe Fiber::ExecutionContext::Monitor do
    describe ".next_interval" do
      it "doubles the interval while idle, up to MAX_EVERY" do
        every = Fiber::ExecutionContext::Monitor::DEFAULT_EVERY
        intervals = Array.new(10) { every = Fiber::ExecutionContext::Monitor.next_interval(every, busy: false) }
        intervals.first.should eq 20.milliseconds
        intervals.should eq intervals.sort
        intervals.last.should eq Fiber::ExecutionContext::Monitor::MAX_EVERY
      end

      it "returns to the default interval when busy" do
        Fiber::ExecutionContext::Monitor.next_interval(1.second, busy: true)
          .should eq Fiber::ExecutionContext::Monitor::DEFAULT_EVERY
      end
    end

    it "backs off while idle" do
      wait_for_monitor_back_off(Fiber::ExecutionContext::Monitor::MAX_EVERY)
    end

    it "returns to the default interval when a thread enters a blocking syscall" do
      wait_for_monitor_back_off(100.milliseconds)
      File.open(__FILE__) { } # open(2) goes through Fiber.syscall
      should_eventually(be_true, 100.milliseconds) do
        Fiber::ExecutionContext.monitor_interval.not_nil! <= 20.milliseconds
      end
    end

    it "hands off a scheduler blocked in a syscall while backed off" do
      ctx = Fiber::ExecutionContext::Parallel.new("monitor-spec", 1)
      wait_for_monitor_back_off(Fiber::ExecutionContext::Monitor::MAX_EVERY)
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
  end
{% end %}
