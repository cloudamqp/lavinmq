require "../logger"

module LavinMQ
  module Clustering
    # Detects a stalled data-dir filesystem on the leader and self-fences so a
    # follower can take over.
    #
    # Background: leadership is held via an etcd lease whose keepalive runs on a
    # network-only isolated thread (see `Etcd::Lease#keepalive_loop`). A pure
    # disk-I/O stall — e.g. the backing block volume hanging — blocks `syncfs(2)`
    # (see `Persister#sync`) in uninterruptible sleep, but leaves the keepalive
    # thread happily renewing the lease over the network. etcd therefore keeps
    # considering this node a healthy leader and no follower is ever promoted;
    # publishers hang on confirms indefinitely with no automatic recovery.
    #
    # This watchdog closes that gap. It periodically probes the data dir with a
    # bounded-timeout write+fsync run on its *own* isolated thread. If a probe
    # does not complete within `timeout` (or returns an error such as EIO / a
    # read-only remount), the disk is considered lost: the `fence` callback runs,
    # which is expected to drop the etcd lease and hard-exit the process so the
    # keepalive stops and a follower is promoted within the lease TTL.
    #
    # Scope: only the leader runs this. A follower with a dead disk already
    # self-heals via the existing ISR mechanism — it stops acking the
    # replication stream and the leader drops it from the ISR after
    # `Follower::ACK_TIMEOUT` — so it can never be a promotion candidate. The
    # leader is the only role lacking such a liveness check.
    class DiskWatchdog
      Log = LavinMQ::Log.for "clustering.disk_watchdog"

      # `probe` performs one disk health check and raises on failure. `fence` is
      # invoked (once) with a human-readable reason when the disk is deemed lost.
      def initialize(@interval : Time::Span, @timeout : Time::Span,
                     @probe : Proc(Nil), &@fence : String ->)
        @fenced = false
        @stopped = false
      end

      # Build a watchdog whose probe writes+fsyncs a canary file in `data_dir`.
      def self.for_data_dir(data_dir : String, interval : Time::Span,
                            timeout : Time::Span, &fence : String ->)
        new(interval, timeout, -> { DiskWatchdog.probe_disk(data_dir) }, &fence)
      end

      # Probe loop. Blocking — spawn it in its own fiber. Runs on the default
      # execution context, which stays responsive during a stall because the
      # blocking syscalls live on isolated threads (the persister's syncfs and
      # this watchdog's own probe thread), not on the worker threads.
      def run : Nil
        until @stopped || @fenced
          sleep @interval
          probe_once unless @stopped
        end
      end

      # Halt the watchdog without fencing. Called on graceful shutdown so a slow
      # but legitimate final flush can't be mistaken for a stall and hard-killed.
      def stop : Nil
        @stopped = true
      end

      private def probe_once : Nil
        result = ::Channel(Exception?).new(1)
        started = Time.instant
        # The probe must run on a separate thread: on a wedged filesystem the
        # fsync(2) blocks in uninterruptible sleep and would freeze whatever
        # thread it runs on. Isolating it keeps this fiber — and therefore the
        # timeout below — responsive. If the fs is stalled this thread leaks,
        # blocked in D state, which is fine: the process is about to be killed.
        Fiber::ExecutionContext::Isolated.new("Disk probe") do
          err = begin
            @probe.call
            nil
          rescue ex
            ex
          end
          result.send(err) rescue nil
        end

        select
        when err = result.receive
          if err
            # A returned error (EIO, ENOSPC, EROFS after a read-only remount, …)
            # is itself grounds to step down: this node cannot durably persist.
            fence!("disk probe failed: #{err.class}: #{err.message}")
          else
            elapsed = Time.instant - started
            Log.debug { "disk probe ok in #{elapsed.total_milliseconds.round(1)}ms" }
          end
        when timeout(@timeout)
          fence!("disk probe did not complete within #{@timeout.total_seconds}s (filesystem stalled)")
        end
      end

      # Write, fsync, and remove a small canary file. fsync(2) is the syscall
      # that hangs on a stalled ext4 journal — the same commit path the
      # persister's syncfs waits on — so its latency is what we bound. A
      # read-only remount surfaces here as EROFS from the open/write.
      def self.probe_disk(data_dir : String) : Nil
        path = File.join(data_dir, ".disk_watchdog")
        begin
          File.open(path, "w") do |f|
            f.print("ok")
            f.flush
            f.fsync
          end
        ensure
          File.delete?(path)
        end
      end

      private def fence!(reason : String) : Nil
        return if @fenced || @stopped
        @fenced = true
        Log.fatal { "Self-fencing: #{reason}" }
        @fence.call(reason)
      end
    end
  end
end
