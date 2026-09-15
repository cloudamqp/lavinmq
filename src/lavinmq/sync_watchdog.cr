require "./config"
require "./logger"

module LavinMQ
  # A disk sync (msync, fsync, syncfs) can block indefinitely on a failing or
  # overloaded device, and nothing else in the node notices: the loop that
  # issued it just never returns. Bound the wait: when a guarded sync exceeds
  # `clustering_sync_timeout` the process exits so a standby node can take
  # over. Standalone there is nothing to fail over to, so it only logs.
  class SyncWatchdog
    Log = LavinMQ::Log.for "sync_watchdog"

    # One signal when a sync starts and one when it ends; capacity two so the
    # guarded fiber never blocks on the watchdog itself.
    @signals = ::Channel(Nil).new(2)

    def initialize(@name : String, *, @exit_on_timeout : Bool)
      Fiber::ExecutionContext::Isolated.new("#{@name} sync watchdog") { run }
    end

    # Runs the block under the timeout. Once closed the block still runs,
    # only unguarded (a tx.commit racing shutdown syncs inline, see
    # Persister#sync).
    def guard(& : -> Nil) : Nil
      begin
        @signals.send nil
      rescue ::Channel::ClosedError
        return yield
      end
      begin
        yield
      ensure
        signal_end
      end
    end

    private def signal_end : Nil
      @signals.send nil
    rescue ::Channel::ClosedError
    end

    def close : Nil
      @signals.close
    end

    protected def timeout : Time::Span
      Config.instance.clustering_sync_timeout
    end

    private def run
      loop do
        @signals.receive # a sync is about to run
        wait
      end
    rescue ::Channel::ClosedError
    end

    # Waits for the sync that just started to signal completion.
    protected def wait : Nil
      select
      when @signals.receive
      when timeout timeout
        if @exit_on_timeout
          Log.fatal { "#{@name}: disk sync blocked for more than #{timeout}, exiting so another node can take over" }
          exit 1
        end
        Log.error { "#{@name}: disk sync blocked for more than #{timeout}" }
        # Consume the real completion so it isn't mistaken for the next start.
        @signals.receive
      end
    end
  end
end
