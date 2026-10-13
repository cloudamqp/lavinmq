# Crystal 1.22.0 initializes `done` to true in Isolated#wait_for, so the
# context resumes its fiber whenever the event loop returns, even when the
# fiber wasn't made ready (the loop got interrupted, or a timer fired).
# The fiber then runs on while its stack-allocated event is still in a
# waiters list, and the next waiter on that fd crashes on the stale event.
# https://github.com/crystal-lang/crystal/issues/17629
# Fixed by https://github.com/crystal-lang/crystal/pull/17630, remove this
# once the Crystal version with that fix is the minimum we support.
{% if compare_versions(Crystal::VERSION, "1.22.0") == 0 %}
  class Fiber::ExecutionContext::Isolated
    private def wait_for(event_loop) : Nil
      loop do
        @mutex.synchronize do
          return if check_enqueued?
          @waiting = true
        end

        done = false

        event_loop.run(blocking: true) do |fiber|
          unless fiber == @main_fiber
            raise RuntimeError.new("Concurrency is disabled in isolated contexts")
          end
          done = true
        end

        break if done
      end

      @mutex.synchronize do
        @waiting = false
        @enqueued = false
      end
    end
  end
{% end %}
