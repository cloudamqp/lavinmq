module LavinMQ
  class Etcd
    # Represents holding a Lease
    # Can be revoked or wait until lost
    class Lease
      getter id, expired

      def initialize(@etcd : Etcd, @id : Int64, ttl : Int32)
        @expired = Channel(Exception?).new
        Fiber::ExecutionContext::Isolated.new("Etcd::Lease") do
          keepalive_loop(ttl)
        end
      end

      # Force release of lease (and leadership)
      def release
        @expired.close
        @etcd.lease_revoke(@id)
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

      private def keepalive_loop(ttl : Int32)
        loop do
          sleep (ttl / 3).seconds
          ttl = @etcd.lease_keepalive(@id)
        end
        error : Exception? = nil
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
