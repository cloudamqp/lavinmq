module LavinMQ
  class Etcd
    # Represents holding a Lease
    # Can be revoked or wait until lost
    class Lease
      getter id

      def initialize(@etcd : Etcd, @id : Int64, ttl : Int32)
        @lost_leadership = Channel(Nil).new
        Fiber::ExecutionContext::Isolated.new("Etcd::Lease") do
          keepalive_loop(ttl)
        end
      end

      # Force release leadership
      def release
        @lost_leadership.close
        @etcd.lease_revoke(@id)
      end

      # Wait until looses leadership
      # Raises `Lost` if lost leadership, otherwise returns after `timeout`
      def wait(timeout : Time::Span) : Nil
        select
        when @lost_leadership.receive?
          raise Lost.new
        when timeout(timeout)
        end
      end

      private def keepalive_loop(ttl : Int32)
        loop do
          sleep (ttl * 0.7).seconds
          ttl = @etcd.lease_keepalive(@id)
        end
      rescue ex
        Log.error(exception: ex) { "Lost leadership" } unless @lost_leadership.closed?
      ensure
        @lost_leadership.close
      end

      class Lost < Exception; end
    end
  end
end
