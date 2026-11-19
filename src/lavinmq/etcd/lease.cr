module LavinMQ
  class Etcd
    # Represents holding a Lease
    # Can be revoked or wait until lost
    class Lease
      getter id, expired

      # A thread of its own, so busy client fibers can't delay a keepalive
      @@keepalive_context = Fiber::ExecutionContext::Concurrent.new("Etcd::Lease")

      def self.grant(etcd : Etcd, id : Int64, ttl : Int32) : self
        lease = new(etcd, id)
        @@keepalive_context.spawn(name: "Etcd::Lease #{id.to_s(16)} keepalive") do
          lease.keepalive_loop(ttl)
        end
        lease
      end

      private def initialize(@etcd : Etcd, @id : Int64)
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
