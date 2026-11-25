module LavinMQ
  class Etcd
    # Represents holding a Lease
    # Can be revoked or wait until lost
    class Lease
      getter id, expired

      # A thread of its own, so busy client fibers can't delay a keepalive
      @@keepalive_context = Fiber::ExecutionContext::Concurrent.new("Etcd::Lease")

      # *started* is when the grant etcd answered was sent, its TTL counts from no earlier
      def self.grant(etcd : Etcd, id : Int64, ttl : Int32, started : Time::Instant = Time.instant) : self
        lease = new(etcd, id, ttl, started)
        @@keepalive_context.spawn(name: "Etcd::Lease #{id.to_s(16)} keepalive") do
          lease.keepalive_loop(ttl)
        end
        lease
      end

      private def initialize(@etcd : Etcd, @id : Int64, ttl : Int32, @started : Time::Instant)
        @expired = Channel(Exception?).new
        @released = Atomic(Bool).new(false)
        # When etcd expires the lease at the earliest, its TTL counts from
        # when it got the last keepalive. Nanoseconds since @started.
        @deadline = Atomic(Int64).new(ttl.seconds.total_nanoseconds.to_i64)
      end

      # Force release of lease (and leadership). The lease is revoked before
      # anything waiting on it wakes, so writes fenced by the lease are already
      # refused by then.
      # Both a stop and the node itself can release it. Each call revokes, so no
      # caller returns before the lease is gone, and a lease already gone counts
      # as released.
      def release
        @released.set(true)
        @etcd.lease_revoke(@id)
      rescue LeaseNotFound
      ensure
        @expired.close
      end

      # Wait until lease expires (if leader, leadership is lost)
      # Raises `Expired` if lease expired, otherwise returns after `timeout`.
      # Also raises once no keepalive has succeeded within the TTL, as a
      # keepalive can block for long in Etcd#with_tcp's retries. That doesn't
      # stop the keepalive loop, so the caller must give up the lease.
      def wait(timeout : Time::Span) : Nil
        wait_until = Time.instant + timeout
        loop do
          now = Time.instant
          deadline = self.deadline
          raise Expired.new("No keepalive succeeded within the lease TTL") if now >= deadline
          return if now >= wait_until
          select
          when err = @expired.receive?
            raise Expired.new(cause: err)
          when timeout({wait_until, deadline}.min - now)
          end
        end
      end

      # The local lease deadline, never later than etcd's expiry
      private def deadline : Time::Instant
        @started + @deadline.get.nanoseconds
      end

      private def past_deadline? : Bool
        Time.instant >= deadline
      end

      # Keepalives go out a third of the TTL after the previous one was sent,
      # not after it was answered, so a slow answer doesn't eat into the margin
      # to the deadline. A failed one is retried sooner.
      # Answers slower than *request_timeout* are cut off even if etcd renewed
      # the lease, so a sustained multi-second etcd latency ends the lease.
      protected def keepalive_loop(ttl : Int32)
        request_timeout = (ttl / 3).seconds
        next_at = @started + (ttl / 3).seconds
        failures = 0
        loop do
          select
          when @expired.receive? # closed by #release
            return
          when timeout({next_at - Time.instant, Time::Span.zero}.max)
          end
          begin
            ttl, sent = @etcd.lease_keepalive(@id, request_timeout)
            @deadline.set((sent + ttl.seconds - @started).total_nanoseconds.to_i64)
            next_at = sent + (ttl / 3).seconds
            Log.info { "Lease #{@id} keepalive succeeded again after #{failures} failed attempts" } if failures > 0
            failures = 0
          rescue ex
            # Only etcd's word that the lease is gone ends it before the
            # deadline, any other error (e.g. "no leader" during an etcd
            # leader election, or EOF while a member restarts) is retried
            raise ex if ex.is_a?(LeaseNotFound | LeaseExpired) || past_deadline?
            failures += 1
            if failures == 1
              Log.warn { "Lease #{@id} keepalive failed, retrying: #{ex.message}" }
            else
              Log.debug { "Lease #{@id} keepalive failed again, retrying: #{ex.message}" }
            end
            next_at = Time.instant + (ttl / 10).seconds
          end
        end
      rescue ex
        unless @expired.closed?
          if @released.get # a keepalive in flight while #release revokes the lease
            Log.debug { "Lease #{@id} released: #{ex.message}" }
          else
            Log.error { "Lease #{@id} expired: #{ex.message}" }
          end
          error = ex
          while @expired.try_send?(error)
          end
        end
      ensure
        @expired.close
      end

      class Expired < Exception; end
    end
  end
end
