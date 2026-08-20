require "../spec_helper"
require "../../src/lavinmq/clustering/control_packet"

module ControlPacketSpec
  describe LavinMQ::Clustering::ControlPacket do
    symbol = LavinMQ::Clustering::SyncPacket::SYMBOL

    # Answers "is this an instruction?" where .from_str answers "which one?".
    describe ".control?" do
      it "is true for a record named by a symbol, with or without an argument" do
        LavinMQ::Clustering::ControlPacket.control?(symbol.to_s).should be_true
        LavinMQ::Clustering::ControlPacket.control?("#{symbol}vhost_dir/msgs.0000001").should be_true
      end

      it "is false for a replicated file path" do
        LavinMQ::Clustering::ControlPacket.control?("vhost_dir/msgs.0000001").should be_false
        LavinMQ::Clustering::ControlPacket.control?("").should be_false
      end
    end

    describe ".from_str" do
      it "returns the packet the symbol stands for, with the rest as its path" do
        packet = LavinMQ::Clustering::ControlPacket.from_str("#{symbol}vhost_dir/msgs.0000001")
        packet.should be_a LavinMQ::Clustering::SyncPacket
        packet.as(LavinMQ::Clustering::SyncPacket).path.should eq "vhost_dir/msgs.0000001"
      end

      # The empty path names the data dir itself: sync the whole filesystem.
      it "returns an empty path for a bare symbol" do
        LavinMQ::Clustering::ControlPacket.from_str(symbol.to_s)
          .as(LavinMQ::Clustering::SyncPacket).path.should eq ""
      end

      it "returns nil for a symbol this build doesn't know" do
        LavinMQ::Clustering::ControlPacket.from_str("%from_the_future").should be_nil
        LavinMQ::Clustering::ControlPacket.from_str("vhost_dir/msgs.0000001").should be_nil
      end
    end

    # Both sides of the wire agree on the format because they share the packet:
    # what the leader writes is what the follower parses back.
    it "parses back the record it writes" do
      io = IO::Memory.new
      packet = LavinMQ::Clustering::SyncPacket.new("vhost_dir/msgs.0000001")
      packet.to_io(io)
      io.size.should eq packet.bytesize # what's counted as sent is what's written

      io.rewind
      len = io.read_bytes(Int32, IO::ByteFormat::LittleEndian)
      LavinMQ::Clustering::ControlPacket.from_str(io.read_string(len))
        .as(LavinMQ::Clustering::SyncPacket).path.should eq "vhost_dir/msgs.0000001"
      io.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 0 # empty body
    end
  end
end
