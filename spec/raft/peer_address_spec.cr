require "../spec_helper"
require "../../src/lavinmq/raft/peer_address"

describe LavinMQ::Raft::PeerAddress do
  describe ".new(host, raft_port, data_port)" do
    it "advertises the same host for both endpoints and round-trips through to_s" do
      addr = LavinMQ::Raft::PeerAddress.new("pub.example.com", 5680, 5679)
      addr.to_s.should eq "pub.example.com:5680,pub.example.com:5679"
    end
  end

  describe ".parse?" do
    it "decodes a packed address into both endpoints" do
      addr = LavinMQ::Raft::PeerAddress.parse?("rhost:5680,dhost:5679").not_nil!
      addr.raft_host.should eq "rhost"
      addr.raft_port.should eq 5680
      addr.data_host.should eq "dhost"
      addr.data_port.should eq 5679
    end

    it "returns nil on a malformed address" do
      LavinMQ::Raft::PeerAddress.parse?("garbage").should be_nil
      LavinMQ::Raft::PeerAddress.parse?("host:5680").should be_nil               # no comma
      LavinMQ::Raft::PeerAddress.parse?("host:5680,host:notaport").should be_nil # bad port
    end
  end

  describe "#raft_endpoint" do
    it "strips IPv6 brackets so the host is dialable by TCPSocket" do
      addr = LavinMQ::Raft::PeerAddress.parse?("[::1]:5680,[::1]:5679").not_nil!
      addr.raft_endpoint.should eq({"::1", 5680})
    end

    it "leaves a regular host untouched" do
      addr = LavinMQ::Raft::PeerAddress.parse?("pub:5680,pub:5679").not_nil!
      addr.raft_endpoint.should eq({"pub", 5680})
    end
  end

  describe "#data_uri" do
    it "builds a tcp:// URI keeping IPv6 brackets so it stays a valid authority" do
      addr = LavinMQ::Raft::PeerAddress.parse?("[::1]:5680,[::1]:5679").not_nil!
      addr.data_uri.should eq "tcp://[::1]:5679"
      URI.parse(addr.data_uri).hostname.should eq "::1"
    end
  end
end
