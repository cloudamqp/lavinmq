require "../spec_helper"
require "../../src/lavinmq/raft/cluster_command"

describe LavinMQ::Raft::ClusterCommand do
  describe "SetSecret" do
    it "round-trips through to_io / from_io" do
      original = LavinMQ::Raft::ClusterCommand::SetSecret.new("hunter2")
      io = IO::Memory.new
      original.to_io(io, IO::ByteFormat::LittleEndian)
      io.rewind
      decoded = LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      decoded.should be_a(LavinMQ::Raft::ClusterCommand::SetSecret)
      decoded.as(LavinMQ::Raft::ClusterCommand::SetSecret).secret.should eq "hunter2"
    end

    it "reports bytesize equal to bytes actually written" do
      cmd = LavinMQ::Raft::ClusterCommand::SetSecret.new("hunter2")
      io = IO::Memory.new
      cmd.to_io(io, IO::ByteFormat::LittleEndian)
      io.pos.should eq cmd.bytesize
    end
  end

  describe "AddToIsr" do
    it "round-trips through to_io / from_io" do
      original = LavinMQ::Raft::ClusterCommand::AddToIsr.new(42_u64)
      io = IO::Memory.new
      original.to_io(io, IO::ByteFormat::LittleEndian)
      io.rewind
      decoded = LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      decoded.should be_a(LavinMQ::Raft::ClusterCommand::AddToIsr)
      decoded.as(LavinMQ::Raft::ClusterCommand::AddToIsr).node_id.should eq 42_u64
    end

    it "reports bytesize equal to bytes actually written" do
      cmd = LavinMQ::Raft::ClusterCommand::AddToIsr.new(42_u64)
      io = IO::Memory.new
      cmd.to_io(io, IO::ByteFormat::LittleEndian)
      io.pos.should eq cmd.bytesize
    end
  end
end
