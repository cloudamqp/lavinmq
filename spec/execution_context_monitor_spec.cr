require "./spec_helper"

{% if (!flag?(:without_mt) && !flag?(:preview_mt) || flag?(:execution_context)) &&
        compare_versions(Crystal::VERSION, "1.21.0") >= 0 &&
        compare_versions(Crystal::VERSION, "1.22.0-dev") < 0 &&
        Crystal::EventLoop.has_constant?(:Polling) %}
  private def monitor
    Fiber::ExecutionContext.monitor?.not_nil!
  end

  # Isolated contexts aren't counted, so polling from one doesn't keep the
  # monitor awake the way polling from the spec fiber would.
  private def wait_until_parked : Bool
    deadline = Time.instant + 2.seconds
    until monitor.parked?
      return false if Time.instant > deadline
      sleep 1.millisecond
    end
    true
  end

  private def monitor_parks? : Bool
    parked = false
    Fiber::ExecutionContext::Isolated.new("monitor-spec-observer") do
      parked = wait_until_parked
    end.wait
    parked
  end

  describe Fiber::ExecutionContext::Monitor do
    it "parks when no parallel scheduler is busy" do
      monitor_parks?.should be_true
    end

    it "wakes up when a scheduler becomes busy" do
      monitor_parks?.should be_true
      # spin without yielding, which keeps this scheduler busy
      deadline = Time.instant + 1.second
      while monitor.parked? && Time.instant < deadline
      end
      monitor.parked?.should be_false
    end

    it "hands off a scheduler that blocks in a syscall after the monitor parked" do
      ctx = Fiber::ExecutionContext::Parallel.new("monitor-spec", 1)
      latency = nil
      Fiber::ExecutionContext::Isolated.new("monitor-spec-driver") do
        next unless wait_until_parked
        done = Channel(Time::Span).new
        # blocks the context's only thread, not just the fiber
        ctx.spawn { Fiber.syscall { Thread.sleep(1.second) } }
        sleep 10.milliseconds
        start = Time.instant
        ctx.spawn { done.send(Time.instant - start) }
        latency = done.receive
      end.wait
      # a monitor that stayed parked wouldn't hand off the scheduler, so the
      # second fiber would wait for the syscall to finish
      latency.should_not be_nil
      latency.not_nil!.should be < 200.milliseconds
    end

    it "stops counting schedulers that shut down" do
      ctx = Fiber::ExecutionContext::Parallel.new("monitor-spec-resize", 2)
      running = Atomic(Int32).new(0)
      threads = Channel(Thread).new(2)
      2.times do
        ctx.spawn do
          running.add(1)
          # spin until both fibers run at the same time, on both schedulers
          deadline = Time.instant + 2.seconds
          until running.get == 2 || Time.instant > deadline
          end
          threads.send Thread.current
        end
      end
      threads.receive.should_not eq threads.receive
      ctx.resize(1)
      monitor_parks?.should be_true
    end
  end
{% end %}
