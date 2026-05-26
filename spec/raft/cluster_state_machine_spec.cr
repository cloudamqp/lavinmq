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
end
