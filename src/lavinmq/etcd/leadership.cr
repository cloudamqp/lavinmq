module LavinMQ
  class Etcd
    # Represents holding a Leadership
    # Can be revoked or wait until lost
    class Leadership
      def initialize(@etcd : Etcd, @lease_id : Int64)
        @lost_leadership = Channel(Nil).new
        etcdctl_path = Process.find_executable("etcdctl") || raise("etcdctl not found")
        etcdctl_args = {"lease", "keep-alive", @lease_id.to_s(16), "--endpoints", @etcd.endpoints.join(',')}
        @keepalive_process = Process.new(etcdctl_path, etcdctl_args, error: Process::Redirect::Inherit)
        spawn(keepalive_process_wait, name: "Etcd lease keepalive #{@lease_id}")
      end

      # Force release leadership
      def release
        @etcd.lease_revoke(@lease_id)
        @lost_leadership.close
        @keepalive_process.terminate
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

      private def keepalive_process_wait
        @keepalive_process.wait
        Log.error { "Lost leadership" } unless @lost_leadership.closed?
      ensure
        @lost_leadership.close
      end
    end
  end
end
