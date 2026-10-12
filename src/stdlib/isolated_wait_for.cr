# Crystal 1.22.0 sets `done = true` before running the event loop in
# `Isolated#wait_for`, so when the event loop returns without a ready fiber
# (it got interrupted), the isolated fiber is resumed anyway. A fiber waiting
# on IO then returns with its event still queued on the fd, and later
# segfaults. Same method as 1.22.0 with `done = false`.
{% if compare_versions(Crystal::VERSION, "1.22.0") >= 0 && compare_versions(Crystal::VERSION, "1.23.0") < 0 %}
  class Fiber::ExecutionContext::Isolated
    private def wait_for(event_loop) : Nil
      loop do
        @mutex.synchronize do
          return if check_enqueued?
          @waiting = true
        end

        done = false

        # wait on the event loop
        event_loop.run(blocking: true) do |fiber|
          # sanity check
          unless fiber == @main_fiber
            raise RuntimeError.new("Concurrency is disabled in isolated contexts")
          end

          # can't just return because EventLoop::LibEvent#run captures the block
          done = true
        end

        # the evloop got interrupted: retry
        break if done
      end

      # cleanup
      @mutex.synchronize do
        @waiting = false
        @enqueued = false
      end
    end
  end
{% end %}
