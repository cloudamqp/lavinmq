require "../spec_helper"
require "file_utils"
require "../../src/lavinmq/raft/coordinator"
require "../../src/lavinmq/raft/server"

private def tmp_data_dir : String
  dir = File.tempname("raft-coordinator-spec")
  Dir.mkdir_p(dir)
  dir
end

describe LavinMQ::Raft::Coordinator do
  it "proposes SetIsr on update_isr" do
    dir = tmp_data_dir
    begin
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      transport = ::Raft::MemoryTransport.new(1_u64)
      server = LavinMQ::Raft::Server.new(
        data_dir: dir, advertised_address: "n:5680,n:5679",
        transport: transport, execution_context: Fiber::ExecutionContext.current,
      )
      transport.start
      server.start
      server.bootstrap.should be_true
      select
      when server.is_leader.when_true.receive
      when timeout(2.seconds); fail "timed out"
      end

      coord = LavinMQ::Raft::Coordinator.new(server)
      coord.update_isr(Set{1, 2})
      deadline = Time.instant + 2.seconds
      until server.isr == Set{1, 2}
        fail "timed out" if Time.instant > deadline
        Fiber.yield
      end

      server.stop
      transport.stop
    ensure
      FileUtils.rm_rf(dir)
    end
  end

  it "generates .clustering_password on the leader and returns it (never the raft log)" do
    dir = tmp_data_dir
    begin
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      transport = ::Raft::MemoryTransport.new(1_u64)
      server = LavinMQ::Raft::Server.new(
        data_dir: dir, advertised_address: "n:5680,n:5679",
        transport: transport, execution_context: Fiber::ExecutionContext.current,
      )
      transport.start
      server.start
      server.bootstrap.should be_true
      select
      when server.is_leader.when_true.receive
      when timeout(2.seconds); fail "timed out"
      end

      coord = LavinMQ::Raft::Coordinator.new(server)
      pw = coord.password
      pw.should_not be_empty
      # Persisted to the file, not the replicated state.
      File.read(File.join(dir, ".clustering_password")).should eq pw
      coord.password.should eq pw # subsequent call reads the same file

      server.stop
      transport.stop
    ensure
      FileUtils.rm_rf(dir)
    end
  end

  it "reads an existing .clustering_password without regenerating" do
    dir = tmp_data_dir
    begin
      File.write(File.join(dir, ".clustering_id"), 1.to_s(36))
      File.write(File.join(dir, ".clustering_password"), "preshared-secret\n")
      transport = ::Raft::MemoryTransport.new(1_u64)
      server = LavinMQ::Raft::Server.new(
        data_dir: dir, advertised_address: "n:5680,n:5679",
        transport: transport, execution_context: Fiber::ExecutionContext.current,
      )
      transport.start
      server.start

      coord = LavinMQ::Raft::Coordinator.new(server)
      # A follower (never bootstrapped, not leader) must still read the
      # pre-shared file rather than fail or generate a divergent secret.
      coord.password.should eq "preshared-secret"

      server.stop
      transport.stop
    ensure
      FileUtils.rm_rf(dir)
    end
  end
end
