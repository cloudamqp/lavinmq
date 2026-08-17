require "../spec_helper"
require "../../src/lavinmq/clustering/control_packet"

module ControlPacketSpec
  describe LavinMQ::Clustering::ControlPacket do
    describe ".from_str" do
      it "returns the packet a record's path stands for" do
        LavinMQ::Clustering::ControlPacket.from_str(LavinMQ::Clustering::SYNC_CONTROL_PATH)
          .should be_a LavinMQ::Clustering::SyncControlPacket
      end

      # A newer leader's instruction: the follower skips it rather than dying,
      # so the stream stays aligned (see Client#control).
      it "returns nil for an unknown instruction" do
        LavinMQ::Clustering::ControlPacket
          .from_str("#{LavinMQ::Clustering::CONTROL_PREFIX}from_the_future").should be_nil
      end
    end

    # Both sides of the wire agree on the path because they share the packet:
    # what the leader writes is what the follower parses back.
    it "parses back the record it writes" do
      io = IO::Memory.new
      LavinMQ::Clustering::SyncControlPacket.new.to_io(io)
      io.rewind
      len = io.read_bytes(Int32, IO::ByteFormat::LittleEndian)
      LavinMQ::Clustering::ControlPacket.from_str(io.read_string(len))
        .should be_a LavinMQ::Clustering::SyncControlPacket
      io.read_bytes(Int64, IO::ByteFormat::LittleEndian).should eq 0 # empty body
    end
  end
end
