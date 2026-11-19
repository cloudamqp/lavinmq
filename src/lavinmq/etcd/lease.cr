module LavinMQ
  class Etcd
    # Represents holding a Lease
    # Can be revoked or wait until lost
    class Lease
      getter id, expired

      # Where every lease's keepalive runs, for the life of the process: on a
      # thread of its own, so busy client fibers can't hold a keepalive up past
      # the TTL. An isolated context per lease did that too, but leaked its
      # event loop's file descriptors when it ended, three per lease.
      @@keepalive_context : Fiber::ExecutionContext::Concurrent? = nil
      @@keepalive_context_lock = Mutex.new

      private def self.keepalive_context : Fiber::ExecutionContext::Concurrent
        @@keepalive_context_lock.synchronize do
          @@keepalive_context ||= Fiber::ExecutionContext::Concurrent.new("Etcd::Lease")
        end
      end

      # A lease etcd granted, kept alive until it's released or expires
      def self.grant(etcd : Etcd, id : Int64, ttl : Int32) : self
        lease = new(etcd, id, ttl)
        keepalive_context.spawn(name: "Etcd::Lease #{id.to_s(16)} keepalive") do
          lease.keepalive_loop(ttl)
        end
        lease
      end

      # Without a keepalive, see .grant
      def initialize(@etcd : Etcd, @id : Int64, ttl : Int32)
        @expired = Channel(Exception?).new
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
      # Raises `Expired` if lease expired, otherwise returns after `timeout`
      def wait(timeout : Time::Span) : Nil
        select
        when err = @expired.receive?
          raise Expired.new(cause: err)
        when timeout(timeout)
        end
      end

      protected def keepalive_loop(ttl : Int32)
        loop do
          select
          when @expired.receive? # closed by #release
            return
          when timeout((ttl / 3).seconds)
          end
          ttl = @etcd.lease_keepalive(@id)
        end
      rescue ex : Etcd::Error # only rescue etcd errors
        unless @expired.closed?
          error = ex
          while @expired.try_send?(error)
          end
        end
        Log.debug(exception: ex) { "Lease expired: '#{ex.message}'" }
      ensure
        @expired.close
      end

      class Expired < Exception; end
    end
  end
end
