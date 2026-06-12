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

    it "round-trips an empty secret" do
      original = LavinMQ::Raft::ClusterCommand::SetSecret.new("")
      io = IO::Memory.new
      original.to_io(io, IO::ByteFormat::LittleEndian)
      io.rewind
      decoded = LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      decoded.as(LavinMQ::Raft::ClusterCommand::SetSecret).secret.should eq ""
    end

    it "round-trips a non-ASCII secret" do
      original = LavinMQ::Raft::ClusterCommand::SetSecret.new("hü€nter🦀")
      io = IO::Memory.new
      original.to_io(io, IO::ByteFormat::LittleEndian)
      io.rewind
      decoded = LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      decoded.as(LavinMQ::Raft::ClusterCommand::SetSecret).secret.should eq "hü€nter🦀"
    end
  end

  describe "SetIsr" do
    it "round-trips an empty set" do
      original = LavinMQ::Raft::ClusterCommand::SetIsr.new(Set(Int32).new)
      io = IO::Memory.new
      original.to_io(io, IO::ByteFormat::LittleEndian)
      io.rewind
      decoded = LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      decoded.should be_a(LavinMQ::Raft::ClusterCommand::SetIsr)
      decoded.as(LavinMQ::Raft::ClusterCommand::SetIsr).node_ids.should be_empty
    end

    it "round-trips a populated set" do
      original = LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2, 99})
      io = IO::Memory.new
      original.to_io(io, IO::ByteFormat::LittleEndian)
      io.rewind
      decoded = LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      decoded.as(LavinMQ::Raft::ClusterCommand::SetIsr).node_ids.should eq Set{1, 2, 99}
    end

    it "reports bytesize equal to bytes actually written" do
      cmd = LavinMQ::Raft::ClusterCommand::SetIsr.new(Set{1, 2})
      io = IO::Memory.new
      cmd.to_io(io, IO::ByteFormat::LittleEndian)
      io.pos.should eq cmd.bytesize
    end
  end

  describe ".from_io error handling" do
    it "raises on unknown schema version" do
      io = IO::Memory.new
      io.write_bytes(99_u8, IO::ByteFormat::LittleEndian) # bogus version
      io.write_bytes(0_u8, IO::ByteFormat::LittleEndian)  # SetSecret tag
      io.rewind
      expect_raises(LavinMQ::Raft::ClusterCommand::InvalidSchemaVersion) do
        LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      end
    end

    it "raises on unknown tag value" do
      io = IO::Memory.new
      io.write_bytes(LavinMQ::Raft::ClusterCommand::SCHEMA_VERSION, IO::ByteFormat::LittleEndian)
      io.write_bytes(99_u8, IO::ByteFormat::LittleEndian) # bogus tag
      io.rewind
      expect_raises(Exception) do
        LavinMQ::Raft::ClusterCommand.from_io(io, IO::ByteFormat::LittleEndian)
      end
    end
  end
end
