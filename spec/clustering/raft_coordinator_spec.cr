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
