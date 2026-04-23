module LavinMQ
  module FiberProfiler
    STACK_DEPTH = 10

    @@duration : Time::Span = 5.seconds
    @@sampling = Atomic(Bool).new(false)

    def self.duration : Time::Span
      @@duration
    end

    def self.duration=(value : Time::Span) : Time::Span
      @@duration = value
    end

    def self.sampling? : Bool
      @@sampling.get(:acquire)
    end

    # Captures the current fiber's call stack, filtering out profiler frames.
    # Called from inside the swapcontext hook so frames containing
    # "fiber_profiler" are stripped before the STACK_DEPTH limit is applied.
    def self.capture_stack : Array(String)
      caller.reject(&.includes?("fiber_profiler")).first(STACK_DEPTH).to_a
    end

    def self.profile(io : IO) : Nil
      unless @@sampling.compare_and_set(false, true, :acquire, :relaxed)[1]
        io.puts "A fiber profile is already running. Try again in a few seconds."
        io.flush
        return
      end

      begin
        Fiber.list do |f|
          f.__profiler_resume_count_reset
          f.__profiler_stack = nil
        end

        io.puts "Sampling fibers for #{@@duration.total_seconds.to_i}s..."
        io.flush

        sleep @@duration

        fibers = [] of {String, Int32, Array(String)?}
        Fiber.list do |f|
          fibers << {f.name || "(unnamed)", f.__profiler_resume_count, f.__profiler_stack}
        end
      ensure
        @@sampling.set(false, :release)
      end

      total = fibers.sum { |_, c, _| c }
      top_n = Math.min(10, fibers.size)
      fibers.sort_by! { |_, c, _| -c }

      io.puts
      io.printf "Top %d fibers — %d context switches in %ds:\n\n", top_n, total, @@duration.total_seconds.to_i
      io.printf "  %-2s  %-50s  %8s  %6s\n", "#", "Fiber", "Resumes", "%"
      io.puts "  #{"─" * 74}"

      fibers.first(top_n).each_with_index do |(name, count, stack), idx|
        pct = total > 0 ? count * 100.0 / total : 0.0
        io.printf "  %-2d  %-50s  %8d  %5.1f%%\n", idx + 1, name, count, pct
        if stack && !stack.empty?
          stack.each { |frame| io.puts "        #{frame}" }
          io.puts
        end
      end
      io.flush
    end
  end
end

class Fiber
  @__profiler_resume_count : Atomic(Int32) = Atomic(Int32).new(0)
  @__profiler_stack : Array(String)? = nil

  def __profiler_resume_count : Int32
    @__profiler_resume_count.get(:relaxed)
  end

  def __profiler_resume_count_add : Nil
    @__profiler_resume_count.add(1, :relaxed)
  end

  def __profiler_resume_count_reset : Nil
    @__profiler_resume_count.set(0, :relaxed)
  end

  def __profiler_stack : Array(String)?
    @__profiler_stack
  end

  def __profiler_stack=(stack : Array(String)?) : Nil
    @__profiler_stack = stack
  end
end

{% if flag?(:execution_context) %}
  # Hook the execution-context scheduler's context switch to track resumes and
  # record where each fiber is suspended. Only active during a profile window.
  module Fiber::ExecutionContext::Scheduler
    def swapcontext(fiber : Fiber) : Nil
      if LavinMQ::FiberProfiler.sampling?
        fiber.__profiler_resume_count_add
        Fiber.current.__profiler_stack = LavinMQ::FiberProfiler.capture_stack
      end
      previous_def(fiber)
    end
  end
{% end %}
