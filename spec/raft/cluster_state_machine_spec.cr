require "../spec_helper"
require "../../src/lavinmq/raft/cluster_state_machine"

describe LavinMQ::Raft::ClusterStateMachine do
  describe "#apply" do
    it "replaces ISR on SetIsr" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2}))
      sm.isr.should eq Set{1, 2}
    end

    it "replaces ISR wholesale on subsequent SetIsr" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2}))
      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{3}))
      sm.isr.should eq Set{3}
    end

    it "clears ISR on SetIsr with empty set" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{7}))
      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set(Int32).new))
      sm.isr.should be_empty
    end
  end

  describe "snapshot/restore" do
    it "round-trips state through snapshot and restore" do
      original = LavinMQ::Raft::ClusterStateMachine.new
      original.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2}))

      io = IO::Memory.new
      original.snapshot(io)
      io.rewind

      restored = LavinMQ::Raft::ClusterStateMachine.new
      restored.restore(io)

      restored.isr.should eq Set{1, 2}
    end

    it "round-trips empty state" do
      original = LavinMQ::Raft::ClusterStateMachine.new
      io = IO::Memory.new
      original.snapshot(io)
      io.rewind

      restored = LavinMQ::Raft::ClusterStateMachine.new
      restored.restore(io)

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

    it "replaces existing state on restore" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{99}))

      source = LavinMQ::Raft::ClusterStateMachine.new
      source.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2}))

      io = IO::Memory.new
      source.snapshot(io)
      io.rewind
      sm.restore(io)

      sm.isr.should eq Set{1, 2}
    end
  end

  describe "state snapshot" do
    it "publishes an immutable snapshot after each apply" do
      sm = LavinMQ::Raft::ClusterStateMachine.new
      sm.state.should eq LavinMQ::Raft::ClusterState::EMPTY

      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{42}))
      first = sm.state
      first.isr.should eq Set{42}

      sm.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2}))
      second = sm.state
      second.isr.should eq Set{1, 2}

      # The earlier captured snapshot is unchanged — proves immutability of the publish.
      first.isr.should eq Set{42}
    end

    it "publishes a snapshot on restore" do
      original = LavinMQ::Raft::ClusterStateMachine.new
      original.apply(LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2}))

      io = IO::Memory.new
      original.snapshot(io)
      io.rewind

      restored = LavinMQ::Raft::ClusterStateMachine.new
      restored.state.should eq LavinMQ::Raft::ClusterState::EMPTY
      restored.restore(io)
      restored.state.isr.should eq Set{1, 2}
    end
  end
end
