require "../spec_helper"
require "raft"
require "../../src/lavinmq/clustering/raft_coordinator"

private def raft_config(data_dir : String, id : Int32, peers : String) : LavinMQ::Config
  config = LavinMQ::Config.new
  config.data_dir = data_dir
  config.clustering = true
  config.clustering_backend = LavinMQ::ClusteringBackend::Raft
  config.clustering_raft_node_id = id
  config.clustering_raft_peers = peers
  config.clustering_raft_bind = "127.0.0.1"
  config.clustering_raft_port = 5680 + id
  config
end

# Run an election in a fiber and return whether leadership was acquired within
# the timeout.
private def became_leader?(coord, timeout : Time::Span = 3.seconds) : Bool
  ch = Channel(Nil).new(1)
  spawn { coord.await_leadership; ch.send(nil) }
  select
  when ch.receive
    true
  when timeout(timeout)
    false
  end
end

describe LavinMQ::Clustering::RaftCoordinator do
  describe "single node" do
    it "elects itself leader on bootstrap" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        begin
          coord.start
          became_leader?(coord).should be_true
        ensure
          coord.release
        end
      end
    end

    it "round-trips the ISR set" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        begin
          coord.start
          became_leader?(coord).should be_true
          coord.isr.should be_nil # nothing recorded yet
          coord.update_isr(Set{1})
          coord.isr.should eq Set{1}
          coord.update_isr(Set{1, 2})
          coord.isr.should eq Set{1, 2}
        ensure
          coord.release
        end
      end
    end

    it "notifies watch_isr on change" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        begin
          coord.start
          became_leader?(coord).should be_true
          seen = Channel(Set(Int32)?).new(4)
          spawn do
            coord.watch_isr do |members|
              seen.send(members)
              break if members == Set{7}
            end
          end
          coord.update_isr(Set{7})
          found = false
          4.times do
            select
            when members = seen.receive
              if members == Set{7}
                found = true
                break
              end
            when timeout(3.seconds)
              break
            end
          end
          found.should be_true
        ensure
          coord.release
        end
      end
    end

    it "publishes and observes the leader URI" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "")
        uri = "tcp://localhost:6001"
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, uri, Raft::MemoryTransport.new(1u64))
        begin
          coord.start
          became_leader?(coord).should be_true
          coord.publish_leader_uri(uri)
          observed = Channel(String?).new(4)
          spawn do
            coord.watch_leader_uri do |u|
              observed.send(u)
              break if u == uri
            end
          end
          found = false
          4.times do
            select
            when u = observed.receive
              if u == uri
                found = true
                break
              end
            when timeout(3.seconds)
              break
            end
          end
          found.should be_true
        ensure
          coord.release
        end
      end
    end

    it "restores the ISR from disk after restart" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        coord.start
        became_leader?(coord).should be_true
        coord.update_isr(Set{1, 5, 9})
        coord.release
        Fiber.yield

        # Re-open on the same data dir: the persisted Raft log replays and the
        # state machine restores the ISR.
        coord2 = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        begin
          coord2.start
          coord2.isr.should eq Set{1, 5, 9}
        ensure
          coord2.release
        end
      end
    end
  end

  describe "configuration" do
    it "reads and strips the replication secret from disk" do
      with_datadir do |dir|
        File.write(File.join(dir, ".replication_secret"), "  s3cr3t\n")
        config = raft_config(dir, 1, "")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        coord.password.should eq "s3cr3t"
      end
    end

    it "rejects a peer entry without a port" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "1@host-without-port")
        expect_raises(ArgumentError, /raft peer/) do
          LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        end
      end
    end

    it "rejects a non-numeric peer id" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "x@127.0.0.1:5680")
        expect_raises(ArgumentError, /raft peer id/) do
          LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        end
      end
    end

    it "accepts bracketed IPv6 peers" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "1@[::1]:5681,2@[::1]:5682")
        # parse_peers runs in the constructor; bracketed IPv6 must not raise.
        LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
      end
    end
  end

  describe "not leader" do
    it "update_isr raises StaleLeadership when this node never wins" do
      with_datadir do |dir|
        # Two configured peers but a standalone transport that can't reach peer
        # 2, so this node campaigns forever and never becomes leader.
        config = raft_config(dir, 1, "1@127.0.0.1:5681,2@127.0.0.1:5682")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        begin
          coord.start
          became_leader?(coord, 1.second).should be_false
          expect_raises(LavinMQ::Clustering::RaftCoordinator::StaleLeadership) do
            coord.update_isr(Set{1})
          end
        ensure
          coord.release
        end
      end
    end
  end

  describe "graceful stop" do
    it "wakes await_leadership_lost on release" do
      with_datadir do |dir|
        config = raft_config(dir, 1, "")
        coord = LavinMQ::Clustering::RaftCoordinator.new(config, 1, "tcp://localhost:6001", Raft::MemoryTransport.new(1u64))
        coord.start
        became_leader?(coord).should be_true
        lost = Channel(Nil).new(1)
        spawn { coord.await_leadership_lost; lost.send(nil) }
        coord.release
        select
        when lost.receive
          # released cleanly
        when timeout(3.seconds)
          fail "await_leadership_lost did not return after release"
        end
      end
    end
  end

  describe "stale leader URI after restart" do
    # A former leader that restarts in isolation (can't reach its peers, so it
    # can't re-win) must not replay its own persisted URI as the live leader,
    # or follow_leader would abort it as a duplicate.
    it "suppresses its own persisted URI while not leading", tags: "slow" do
      with_datadir do |dir1|
        with_datadir do |dir2|
          peers = "1@127.0.0.1:5691,2@127.0.0.1:5692"
          transports = Raft::MemoryTransport.mesh([1u64, 2u64])
          uris = {1 => "tcp://localhost:6001", 2 => "tcp://localhost:6002"}
          dirs = {1 => dir1, 2 => dir2}
          coords = {} of Int32 => LavinMQ::Clustering::RaftCoordinator
          {1, 2}.each do |id|
            coords[id] = LavinMQ::Clustering::RaftCoordinator.new(
              raft_config(dirs[id], id, peers), id, uris[id], transports[id.to_u64])
          end
          leader_id = 0
          begin
            coords.each_value &.start
            elected = Channel(Int32).new(2)
            coords.each { |id, c| spawn { c.await_leadership; elected.send(id) } }
            leader_id =
              select
              when id = elected.receive
                id
              when timeout(5.seconds)
                fail "no leader elected"
              end
            # Wait until the leader's own URI is committed+applied (and thus
            # persisted) by observing it through its own watch.
            published = Channel(Nil).new(1)
            spawn do
              coords[leader_id].watch_leader_uri do |u|
                if u == uris[leader_id]
                  published.send(nil)
                  break
                end
              end
            end
            select
            when published.receive
            when timeout(5.seconds)
              fail "leader never published its URI"
            end
          ensure
            coords.each_value &.release
          end
          Fiber.yield

          # Restart the former leader alone: a standalone transport can't reach
          # the peer, so it campaigns forever and never becomes leader.
          restarted = LavinMQ::Clustering::RaftCoordinator.new(
            raft_config(dirs[leader_id], leader_id, peers), leader_id, uris[leader_id], Raft::MemoryTransport.new(leader_id.to_u64))
          begin
            restarted.start
            observed = Channel(String?).new(8)
            spawn { restarted.watch_leader_uri { |u| observed.send(u) } }
            # Over a short window, it must never report its own (stale) URI.
            saw_self = false
            done = false
            until done
              select
              when u = observed.receive
                saw_self = true if u == uris[leader_id]
              when timeout(1.second)
                done = true
              end
            end
            saw_self.should be_false
          ensure
            restarted.release
          end
        end
      end
    end
  end

  describe "multi node" do
    it "elects exactly one leader and fails over on release", tags: "slow" do
      with_datadir do |dir1|
        with_datadir do |dir2|
          with_datadir do |dir3|
            peers = "1@127.0.0.1:5681,2@127.0.0.1:5682,3@127.0.0.1:5683"
            transports = Raft::MemoryTransport.mesh([1u64, 2u64, 3u64])
            dirs = {1 => dir1, 2 => dir2, 3 => dir3}
            coords = {} of Int32 => LavinMQ::Clustering::RaftCoordinator
            (1..3).each do |id|
              config = raft_config(dirs[id], id, peers)
              coords[id] = LavinMQ::Clustering::RaftCoordinator.new(
                config, id, "tcp://localhost:600#{id}", transports[id.to_u64])
            end
            begin
              coords.each_value &.start

              leader = Channel(Int32).new(3)
              coords.each do |id, c|
                spawn { c.await_leadership; leader.send(id) }
              end

              first_leader =
                select
                when id = leader.receive
                  id
                when timeout(5.seconds)
                  fail "no leader elected"
                end

              # Releasing the leader must let another node take over.
              coords[first_leader].release
              select
              when id = leader.receive
                id.should_not eq first_leader
              when timeout(5.seconds)
                fail "no failover after leader released"
              end
            ensure
              coords.each_value &.release
            end
          end
        end
      end
    end
  end
end
