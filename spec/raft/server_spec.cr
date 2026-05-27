require "../spec_helper"
require "file_utils"
require "../../src/lavinmq/raft/server"

private def tmp_data_dir : String
  dir = File.tempname("raft-server-spec")
  Dir.mkdir_p(dir)
  dir
end

private def build_single_node : LavinMQ::Raft::Server
  dir = tmp_data_dir
  File.write(File.join(dir, ".clustering_id"), "1")
  transport = ::Raft::MemoryTransport.new(1_u64)
  server = LavinMQ::Raft::Server.new(
    data_dir: dir,
    advertised_address: "node1:5680,node1:5679",
    transport: transport,
    execution_context: Fiber::ExecutionContext.current,
  )
  transport.start
  server.start
  server
end

describe LavinMQ::Raft::Server do
  describe "node_id" do
    it "generates and persists across reconstruction with the same data_dir" do
      dir = tmp_data_dir
      begin
        t1 = ::Raft::MemoryTransport.new(1_u64)
        s1 = LavinMQ::Raft::Server.new(
          data_dir: dir,
          advertised_address: "n:5680,n:5679",
          transport: t1,
        )
        first_id = s1.node_id
        s1.stop

        t2 = ::Raft::MemoryTransport.new(1_u64)
        s2 = LavinMQ::Raft::Server.new(
          data_dir: dir,
          advertised_address: "n:5680,n:5679",
          transport: t2,
        )
        s2.node_id.should eq first_id
        s2.stop
      ensure
        FileUtils.rm_rf(dir)
      end
    end
  end

  describe "single node" do
    it "stays follower until bootstrap" do
      server = build_single_node
      begin
        server.is_leader.value.should be_false
      ensure
        server.stop
      end
    end

    it "becomes leader after bootstrap and fires is_leader.when_true" do
      server = build_single_node
      begin
        server.bootstrap.should be_true
        select
        when server.is_leader.when_true.receive
          # became leader
        when timeout(2.seconds)
          fail "timed out waiting for leadership"
        end
        server.is_leader.value.should be_true
      ensure
        server.stop
      end
    end
  end
end
