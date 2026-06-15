require "../../spec_helper"
require "../../../src/lavinmq/clustering/controller"

# End-to-end multi-node test of the real Controller.run path: three full
# Controllers (each a VR::Node + control mesh + replication Server + follower
# Client) elect a leader over loopback, replicate data, fail over when the
# leader is stopped, and self-fence when a leader loses its quorum. This is the
# only spec that drives Controller.run rather than the Server/Client directly.
module ControllerIntegrationSpec
  alias Controller = LavinMQ::Clustering::Controller

  # Records leadership loss instead of exiting the process, so a deposed or
  # fenced leader is observable in-process.
  class TestController < Controller
    @lost = Atomic(Int32).new(0)

    protected def on_leadership_lost : Nil
      @lost.set(1)
    end

    def lost_leadership? : Bool
      @lost.get == 1
    end
  end

  record TestServer, controller : TestController, dir : String, port : Int32, id : Int32 do
    # The clustering role as reported by the VR node (how the HTTP status
    # endpoint discovers the primary). Nil if the node was never wired.
    def role : String?
      controller.replicator.vr_node.try(&.status[:role])
    end

    def primary? : Bool
      role == "primary"
    end

    def run : Nil
      spawn(name: "controller #{id} run") { controller.run { } }
    end

    def stop : Nil
      controller.stop
    end
  end

  def self.make_config(data_dir : String, port : Int32, roster : String, secret : String) : LavinMQ::Config
    c = LavinMQ::Config.instance.dup
    c.data_dir = data_dir
    c.clustering = true
    c.clustering_members = roster
    c.clustering_advertised_uri = "tcp://127.0.0.1:#{port}"
    c.clustering_bind = "127.0.0.1"
    c.clustering_port = port
    c.clustering_secret = secret
    c.clustering_heartbeat_interval_ms = 100
    c.clustering_view_change_timeout_ms = 800
    # Keep the data-plane listeners off fixed ports so three nodes coexist in one
    # process: the follower proxies bind ephemeral ports (never connected to in
    # this test) and the metrics server is disabled.
    c.amqp_port = 0
    c.http_port = 0
    c.mqtt_port = 0
    c.unix_path = ""
    c.http_unix_path = ""
    c.mqtt_unix_path = ""
    c.metrics_http_port = -1
    c
  end

  def self.build_cluster(base : String) : Array(TestServer)
    # Allocate three free ports, then release them; the Controllers rebind
    # (TCPServer uses reuse_address) so the roster URIs are stable and known up
    # front — the control mesh needs concrete peer ports to dial.
    holders = (1..3).map { TCPServer.new("127.0.0.1", 0) }
    ports = holders.map(&.local_address.port)
    holders.each &.close
    roster = (1..3).map { |i| "#{i}=tcp://127.0.0.1:#{ports[i - 1]}" }.join(",")
    (1..3).map do |i|
      dir = File.join(base, "n#{i}")
      Dir.mkdir_p dir
      ctrl = TestController.new(make_config(dir, ports[i - 1], roster, "testsecret"))
      TestServer.new(ctrl, dir, ports[i - 1], i)
    end
  end

  # The single node all live nodes agree is primary, or nil if not converged.
  def self.find_primary(nodes : Array(TestServer)) : TestServer?
    ps = nodes.select(&.primary?)
    ps.size == 1 ? ps.first : nil
  end

  describe LavinMQ::Clustering::Controller do
    it "elects a primary, replicates data, and fails over with data intact" do
      with_datadir do |base|
        nodes = build_cluster(base)
        nodes.each &.run

        # One primary; the other two connect to it as synced data followers.
        wait_for(15.seconds) { find_primary(nodes) }
        primary = find_primary(nodes).not_nil!
        wait_for(15.seconds) { primary.controller.replicator.followers.size == 2 }

        # Replicate a record from the primary; a quorum must apply it durably and
        # it must land in both followers' data dirs.
        primary.controller.replicator.append_bytes(File.join(primary.dir, "seed"), "hello".to_slice, 0i64)
        primary.controller.replicator.wait_for_followers
        backups = nodes.reject { |n| n.id == primary.id }
        backups.each do |b|
          wait_for(10.seconds) { File.exists?(File.join(b.dir, "seed")) }
          File.read(File.join(b.dir, "seed")).should eq "hello"
        end

        # Stop the primary: the two survivors must elect a different primary,
        # which holds the replicated data from when it was a follower.
        primary.stop
        survivors = nodes.reject { |n| n.id == primary.id }
        wait_for(15.seconds) do
          np = find_primary(survivors)
          np && np.id != primary.id
        end
        new_primary = find_primary(survivors).not_nil!
        File.read(File.join(new_primary.dir, "seed")).should eq "hello"

        # The remaining survivor re-follows the new primary, and fresh writes
        # replicate to it.
        wait_for(15.seconds) { new_primary.controller.replicator.followers.size == 1 }
        new_primary.controller.replicator.append_bytes(File.join(new_primary.dir, "seed2"), "world".to_slice, 0i64)
        new_primary.controller.replicator.wait_for_followers
        follower = survivors.find! { |n| n.id != new_primary.id }
        wait_for(10.seconds) { File.exists?(File.join(follower.dir, "seed2")) }
        File.read(File.join(follower.dir, "seed2")).should eq "world"
      ensure
        nodes.try &.each { |n| n.stop rescue nil }
      end
    end

    it "lets a restarted node rejoin from persisted state and re-sync" do
      with_datadir do |base|
        nodes = build_cluster(base)
        nodes.each &.run

        wait_for(15.seconds) { find_primary(nodes) }
        primary = find_primary(nodes).not_nil!
        wait_for(15.seconds) { primary.controller.replicator.followers.size == 2 }

        # A durable record: write it on the leader's disk and replicate the whole
        # file (replace_file registers it in the file index, so a later full_sync
        # carries it — unlike append_bytes which only streams bytes).
        File.write(File.join(primary.dir, "rec"), "v1")
        primary.controller.replicator.replace_file(File.join(primary.dir, "rec"))
        primary.controller.replicator.wait_for_followers

        # Restart a backup: stop it (the cluster stays available — primary + the
        # other backup is still a quorum), then bring a fresh Controller up on the
        # SAME data dir and port, simulating a process restart.
        victim = nodes.find! { |n| n.id != primary.id }
        wait_for(10.seconds) { File.exists?(File.join(victim.dir, "rec")) }
        victim.stop

        roster = (1..3).map { |i| "#{i}=tcp://127.0.0.1:#{nodes[i - 1].port}" }.join(",")
        restarted = TestServer.new(
          TestController.new(make_config(victim.dir, victim.port, roster, "testsecret")),
          victim.dir, victim.port, victim.id)
        restarted.run

        # It rejoins as a follower of the primary and a fresh write reaches it,
        # while the record it already held is preserved across the restart.
        wait_for(15.seconds) { primary.controller.replicator.followers.size == 2 }
        File.write(File.join(primary.dir, "rec2"), "v2")
        primary.controller.replicator.replace_file(File.join(primary.dir, "rec2"))
        primary.controller.replicator.wait_for_followers

        wait_for(10.seconds) { File.exists?(File.join(restarted.dir, "rec2")) }
        File.read(File.join(restarted.dir, "rec2")).should eq "v2"
        File.read(File.join(restarted.dir, "rec")).should eq "v1"
      ensure
        nodes.try &.each { |n| n.stop rescue nil }
        restarted.try &.stop rescue nil
      end
    end

    it "fences a primary that loses contact with a quorum (steps down)" do
      with_datadir do |base|
        nodes = build_cluster(base)
        nodes.each &.run

        wait_for(15.seconds) { find_primary(nodes) }
        primary = find_primary(nodes).not_nil!
        wait_for(15.seconds) { primary.controller.replicator.followers.size == 2 }

        # Take both backups down: the primary is now 1 of 3, below quorum(2), and
        # must step down rather than keep serving as a stale leader (the split-
        # brain fence that replaced the etcd lease).
        nodes.each { |n| n.stop if n.id != primary.id }
        wait_for(15.seconds) { primary.controller.lost_leadership? }
        primary.controller.lost_leadership?.should be_true
      ensure
        nodes.try &.each { |n| n.stop rescue nil }
      end
    end
  end
end
