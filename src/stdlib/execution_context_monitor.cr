# Lets the execution context monitor (the "SYSMON" thread) sleep while the
# process is idle.
#
# The stdlib monitor wakes every 10ms for the life of the process, 100 wakeups
# per second, even when the process has nothing to do. That is most of an idle
# LavinMQ's CPU usage, which adds up when many brokers share a host.
#
# The monitor only has work while a Parallel scheduler is busy (running or
# looking for fibers): detaching it from a thread blocked in a syscall, waking
# more schedulers for its queued fibers. So each Parallel scheduler counts
# itself in an atomic while it's busy, and uncounts itself while it waits on
# the event loop or is parked. The monitor keeps its 10ms ticks while there's
# activity, and parks after a whole tick without any busy scheduler. The
# scheduler that raises the count from zero wakes it, so a scheduler that then
# blocks in a syscall is still handed off within ~10ms.
#
# A scheduler under steady load can alternate between busy and idle thousands
# of times per second. The atomic also counts how many times a scheduler became
# busy, and a tick that sees it change doesn't park, so under load the monitor
# ticks like the stock one and nothing signals it.
#
# Isolated contexts aren't counted, the monitor never detaches their thread.
#
# It hooks into private parts of the stdlib and the epoll/kqueue event loop, so
# it is only applied on the Crystal versions it has been verified against.
# Other versions and event loops get the stock monitor. Re-verify against
# `src/fiber/execution_context/{monitor,parallel,parallel/scheduler}.cr` and
# `src/crystal/event_loop/polling.cr` before extending the version range.
{% if (!flag?(:without_mt) && !flag?(:preview_mt) || flag?(:execution_context)) &&
        compare_versions(Crystal::VERSION, "1.21.0") >= 0 &&
        compare_versions(Crystal::VERSION, "1.22.0-dev") < 0 &&
        Crystal::EventLoop.has_constant?(:Polling) %}
  module Fiber::ExecutionContext
    # :nodoc:
    def self.monitor? : Monitor?
      @@monitor
    end

    class Monitor
      # Low 32 bits: number of busy Parallel schedulers. High 32 bits: how many
      # times a scheduler became busy (wrapping).
      @@busy_schedulers = Atomic(UInt64).new(0_u64)
      BUSY_COUNT_MASK = 0xffff_ffff_u64
      BECAME_BUSY     = (1_u64 << 32) | 1_u64

      @park_mutex = Thread::Mutex.new
      @park_condition = Thread::ConditionVariable.new
      @parked = Atomic(Bool).new(false)

      # :nodoc:
      def self.scheduler_busy : Nil
        if (@@busy_schedulers.add(BECAME_BUSY, :sequentially_consistent) & BUSY_COUNT_MASK).zero?
          ExecutionContext.monitor?.try &.wake
        end
      end

      # :nodoc:
      def self.scheduler_idle : Nil
        @@busy_schedulers.sub(1_u64, :sequentially_consistent)
      end

      # :nodoc:
      def parked? : Bool
        @parked.get(:relaxed)
      end

      # Pairs with `#park`: either the monitor sees the incremented count and
      # doesn't wait, or we see it parked and signal it.
      protected def wake : Nil
        return unless @parked.get(:sequentially_consistent)
        @park_mutex.synchronize { @park_condition.signal }
      end

      private def run_loop : Nil
        remaining = @every
        previous = @@busy_schedulers.get(:relaxed)
        loop do
          # unchanged and zero: no scheduler was busy since the previous tick
          if (current = @@busy_schedulers.get(:relaxed)) == previous && (current & BUSY_COUNT_MASK).zero?
            park
            remaining = @every
            current = @@busy_schedulers.get(:relaxed)
          end
          previous = current
          Thread.sleep(remaining)

          start = Crystal::System::Time.instant
          transfer_schedulers_blocked_on_syscall
          increase_parallelism(start)
          collect_stacks(start)
          remaining = (start + @every - Crystal::System::Time.instant).clamp(Time::Span.zero..)
        rescue exception
          Crystal.print_error_buffered("BUG: %s#run_loop crashed", self.class.name, exception: exception)
        end
      end

      # Waits until a Parallel scheduler is busy, or until the next stack
      # collection if there are stacks to collect.
      private def park : Nil
        @park_mutex.synchronize do
          @parked.set(true, :sequentially_consistent)
          if (@@busy_schedulers.get(:sequentially_consistent) & BUSY_COUNT_MASK).zero?
            if collectable_stacks?
              timeout = @collect_stacks_next - Crystal::System::Time.instant
              @park_condition.wait(@park_mutex, timeout) { } if timeout.positive?
            else
              @park_condition.wait(@park_mutex)
            end
          end
          @parked.set(false, :relaxed)
        end
      end

      private def collectable_stacks? : Bool
        ExecutionContext.each do |execution_context|
          if pool = execution_context.stack_pool?
            # StackPool#collect frees half of the pool, rounded down
            return true if pool.lazy_size > 1
          end
        end
        false
      end
    end

    class Parallel
      protected def park_thread(&) : Fiber?
        scheduler = ExecutionContext::Scheduler.current?.as?(Parallel::Scheduler)
        fiber = previous_def do
          found = yield
          scheduler.try &.monitor_idle unless found
          found
        end
        scheduler.try &.monitor_busy
        fiber
      end

      class Scheduler
        # Only accessed by the thread running the scheduler.
        @monitor_busy = false

        # The default context's first scheduler is attached to the main thread,
        # which is busy running the main fiber long before its run loop starts.
        protected def running! : Nil
          previous_def
          monitor_busy
        end

        protected def run_loop : Nil
          monitor_busy
          previous_def
        end

        # :nodoc:
        def monitor_busy : Nil
          return if @monitor_busy
          @monitor_busy = true
          Monitor.scheduler_busy
        end

        # :nodoc:
        def monitor_idle : Nil
          return unless @monitor_busy
          @monitor_busy = false
          Monitor.scheduler_idle
        end
      end
    end
  end

  abstract class Crystal::EventLoop::Polling < Crystal::EventLoop
    def run(queue : Fiber::List*, blocking : Bool) : Nil
      if blocking && (scheduler = Fiber::ExecutionContext::Scheduler.current?.as?(Fiber::ExecutionContext::Parallel::Scheduler))
        scheduler.monitor_idle
        begin
          previous_def
        ensure
          scheduler.monitor_busy
        end
      else
        previous_def
      end
    end

    # Called by a scheduler's run loop right before it shuts down.
    def unregister(scheduler : Fiber::ExecutionContext::Scheduler) : Nil
      scheduler.as?(Fiber::ExecutionContext::Parallel::Scheduler).try &.monitor_idle
      super
    end
  end
{% end %}
