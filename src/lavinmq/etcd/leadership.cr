module LavinMQ
  class Etcd
    # Represents holding a Leadership
    # Can be revoked or wait until lost
    class Leadership
      def initialize(@etcd : Etcd, @lease_id : Int64)
        @lost_leadership = Channel(Nil).new
        spawn(keepalive_loop, name: "Etcd lease keepalive #{@lease_id}")
      end

      # Force release leadership
      def release
        @etcd.lease_revoke(@lease_id)
        @lost_leadership.close
      end

      # Wait until looses leadership
      # Returns true when lost leadership, false when timeout occured
      def wait(timeout : Time::Span) : Bool
        select
        when @lost_leadership.receive?
          true
        when timeout(timeout)
          false
        end
      end

      private def keepalive_loop
        ttl = @etcd.lease_ttl(@lease_id)
        loop do
          sleep (ttl * 0.7).seconds
          ttl = @etcd.lease_keepalive(@lease_id)
        end
      rescue ex
        Log.error(exception: ex) { "Lost leadership" } unless @lost_leadership.closed?
      ensure
        @lost_leadership.close
      end
    end
  end
end
