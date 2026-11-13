require "../../stdlib/isolated_wait_for"
require "sync/exclusive"

module LavinMQ
  class Etcd
    # Represents holding a Lease
    # Can be revoked or wait until lost
    class Lease
      getter id, expired

      def initialize(@etcd : Etcd, @id : Int64, ttl : Int32)
        @expired = Channel(Exception?).new
        # When etcd expires the lease at the earliest, its TTL counts from
        # when it got the last keepalive
        @deadline = Sync::Exclusive(Time::Instant).new(Time.instant + ttl.seconds)
        Fiber::ExecutionContext::Isolated.new("Etcd::Lease") do
          keepalive_loop(ttl)
        end
      end

      # Force release of lease (and leadership). The lease is revoked before
      # anything waiting on it wakes, so writes fenced by the lease are already
      # refused by then.
      def release
        @etcd.lease_revoke(@id)
      ensure
        @expired.close
      end

      # Wait until lease expires (if leader, leadership is lost)
      # Raises `Expired` if lease expired, otherwise returns after `timeout`.
      # Also raises once no keepalive has succeeded within the TTL, as a
      # keepalive can block for long in Etcd#with_tcp's retries.
      def wait(timeout : Time::Span) : Nil
        wait_until = Time.instant + timeout
        loop do
          now = Time.instant
          deadline = @deadline.get
          raise Expired.new("No keepalive succeeded within the lease TTL") if now >= deadline
          return if now >= wait_until
          select
          when err = @expired.receive?
            raise Expired.new(cause: err)
          when timeout({wait_until, deadline}.min - now)
          end
        end
      end

      private def keepalive_loop(ttl : Int32)
        loop do
          select
          when @expired.receive? # closed by #release
            return
          when timeout((ttl / 3).seconds)
          end
          sent = Time.instant
          begin
            ttl = @etcd.lease_keepalive(@id)
            @deadline.set(sent + ttl.seconds)
          rescue ex
            # Only etcd's word that the lease is gone ends it before the
            # deadline, any other error (e.g. "no leader" during an etcd
            # leader election, or EOF while a member restarts) is retried
            raise ex if ex.is_a?(LeaseNotFound | LeaseExpired) || Time.instant > @deadline.get
            Log.warn { "Lease #{@id} keepalive failed, retrying: #{ex.message}" }
            ttl = 1
          end
        end
      rescue ex
        unless @expired.closed?
          Log.error { "Lease #{@id} expired: #{ex.message}" }
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
