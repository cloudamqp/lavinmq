# Adaptive wake-up interval for the execution context monitor (the "SYSMON"
# thread).
#
# The stdlib monitor wakes every 10ms for the life of the process, 100 wakeups
# per second, even when the process has nothing to do. That is most of an idle
# LavinMQ's CPU usage (~0.25-0.5% of a core per process), which adds up when
# many brokers share a host.
#
# This patch backs the interval off exponentially, up to MAX_EVERY, while the
# monitor has nothing to do: no scheduler is in a blocking syscall and no
# parallel context has queued fibers. It returns to 10ms as soon as it has. A
# thread that enters a blocking syscall (`Fiber.syscall`: `open(2)`,
# `getaddrinfo`) wakes a backed-off monitor right away, so a scheduler blocked
# in a syscall is still handed off to another thread within ~10ms.
#
# It reimplements private parts of the stdlib monitor, so it is only applied on
# the Crystal versions it has been verified against. Other versions get the
# stock monitor. Re-verify against `src/fiber/execution_context/monitor.cr`
# before extending the version range.
{% if (!flag?(:without_mt) && !flag?(:preview_mt) || flag?(:execution_context)) &&
        compare_versions(Crystal::VERSION, "1.21.0") >= 0 &&
        compare_versions(Crystal::VERSION, "1.22.0-dev") < 0 %}
  module Fiber::ExecutionContext
    # :nodoc:
    def self.wake_monitor : Nil
      @@monitor.try &.wake
    end

    # :nodoc:
    def self.monitor_interval : Time::Span?
      @@monitor.try &.interval
    end

    module Scheduler
      protected def enter_syscall : UInt32
        value = previous_def
        ExecutionContext.wake_monitor
        value
      end
    end

    class Monitor
      MAX_EVERY = 1.second

      @wake_mutex = Thread::Mutex.new
      @wake_cond = Thread::ConditionVariable.new
      @backed_off = Atomic(Bool).new(false)
      @interval = Atomic(Int64).new(DEFAULT_EVERY.total_nanoseconds.to_i64)

      # Current sleep interval, exposed for specs.
      def interval : Time::Span
        @interval.get(:relaxed).nanoseconds
      end

      # Back off exponentially while idle, reset as soon as there's work.
      def self.next_interval(every : Time::Span, busy : Bool) : Time::Span
        busy ? DEFAULT_EVERY : Math.min(every * 2, MAX_EVERY)
      end

      # Called by threads entering a blocking syscall. Only takes the lock when
      # the monitor is backed off, so the common case is one atomic load.
      def wake : Nil
        return unless @backed_off.get(:sequentially_consistent)
        @wake_mutex.synchronize { @wake_cond.signal }
      end

      private def run_loop : Nil
        every = DEFAULT_EVERY
        loop do
          woken = wait(every)
          now = Crystal::System::Time.instant
          busy = transfer_schedulers_blocked_on_syscall_and_check_busy
          increase_parallelism(now)
          collect_stacks(now)
          every = self.class.next_interval(every, busy || woken)
          @interval.set(every.total_nanoseconds.to_i64, :relaxed)
        rescue exception
          Crystal.print_error_buffered("BUG: %s#run_loop crashed", self.class.name, exception: exception)
        end
      end

      # Sleeps for *span*. When backed off (span > DEFAULT_EVERY) the sleep can
      # be cut short by `#wake`; returns true if it was.
      private def wait(span : Time::Span) : Bool
        if span <= DEFAULT_EVERY
          Thread.sleep(span)
          return false
        end

        woken = true
        @wake_mutex.synchronize do
          @backed_off.set(true, :sequentially_consistent)
          # A thread may have entered a syscall before it could see
          # @backed_off: check after setting it, so the wake-up can't be lost.
          unless any_scheduler_in_syscall?
            @wake_cond.wait(@wake_mutex, span) { woken = false }
          end
          @backed_off.set(false, :relaxed)
        end
        woken
      end

      private def any_scheduler_in_syscall? : Bool
        ExecutionContext.each do |execution_context|
          execution_context.each_scheduler do |scheduler|
            return true if scheduler.syscall_flag?
          end
        end
        false
      end

      # Same as the stdlib `#transfer_schedulers_blocked_on_syscall`, but also
      # reports whether there was anything for the monitor to do.
      private def transfer_schedulers_blocked_on_syscall_and_check_busy : Bool
        busy = false
        ExecutionContext.each do |execution_context|
          execution_context.each_scheduler do |scheduler|
            next unless scheduler.detach_syscall?
            busy = true

            Crystal.trace :sched, "reassociate",
              scheduler: scheduler,
              syscall: scheduler.thread.current_fiber

            pool = ExecutionContext.thread_pool
            pool.detach(scheduler.thread)
            pool.checkout(scheduler)
          end

          if execution_context.is_a?(Parallel)
            busy = true unless execution_context.@global_queue.size.zero?
            execution_context.each_scheduler do |scheduler|
              busy = true unless scheduler.@runnables.empty?
            end
          end
        end
        busy
      end
    end
  end
{% end %}
