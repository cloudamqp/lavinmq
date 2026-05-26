require "../spec_helper"
require "../../src/lavinmq/raft/cluster_state_machine"

describe LavinMQ::Raft::ClusterStateMachine do
  describe "#apply" do
    it "stores the secret on SetSecret" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::SetSecret.new("hunter2"))
      sm.secret.should eq "hunter2"
    end

    it "replaces the secret on a subsequent SetSecret" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::SetSecret.new("first"))
      sm.apply(LavinMQ::Raft::ClusterCommand::SetSecret.new("second"))
      sm.secret.should eq "second"
    end

    it "adds a node id on AddToIsr" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::AddToIsr.new(7_u64))
      sm.isr.should eq Set{7_u64}
    end

    it "is a no-op when AddToIsr is applied for a node already present" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::AddToIsr.new(7_u64))
      sm.apply(LavinMQ::Raft::ClusterCommand::AddToIsr.new(7_u64))
      sm.isr.should eq Set{7_u64}
    end

    it "removes a node id on RemoveFromIsr" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::AddToIsr.new(7_u64))
      sm.apply(LavinMQ::Raft::ClusterCommand::RemoveFromIsr.new(7_u64))
      sm.isr.should be_empty
    end

    it "is a no-op when RemoveFromIsr is applied for an absent node" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::RemoveFromIsr.new(7_u64))
      sm.isr.should be_empty
    end
  end

  describe "snapshot/restore" do
    it "round-trips state through snapshot and restore" do
      original = LavinMQ::Raft::ClusterStateMachine.new
      original.apply(LavinMQ::Raft::ClusterCommand::SetSecret.new("hunter2"))
      original.apply(LavinMQ::Raft::ClusterCommand::AddToIsr.new(1_u64))
      original.apply(LavinMQ::Raft::ClusterCommand::AddToIsr.new(2_u64))

      io = IO::Memory.new
      original.snapshot(io)
      io.rewind

      restored = LavinMQ::Raft::ClusterStateMachine.new
      restored.restore(io)

      restored.secret.should eq "hunter2"
      restored.isr.should eq Set{1_u64, 2_u64}
    end

    it "round-trips empty state" do
      original = LavinMQ::Raft::ClusterStateMachine.new
      io = IO::Memory.new
      original.snapshot(io)
      io.rewind

      restored = LavinMQ::Raft::ClusterStateMachine.new
      restored.restore(io)

      restored.secret.should eq ""
      restored.isr.should be_empty
    end

    it "raises on unknown snapshot version" do
      io = IO::Memory.new
      io.write_bytes(99_u8, IO::ByteFormat::LittleEndian)
      io.rewind
      sm = LavinMQ::Raft::ClusterStateMachine.new
      expect_raises(LavinMQ::Raft::ClusterStateMachine::InvalidSnapshotVersion) do
        sm.restore(io)
      end
    end
  end
end
