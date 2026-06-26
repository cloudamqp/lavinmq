require "spec"
require "../src/lavinmq/connection_info"

describe LavinMQ::ConnectionInfo::IPAddress do
  describe "#address" do
    it "unmaps IPv4-mapped IPv6 addresses" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:127.0.0.1", 1234))

      addr.address.should eq "127.0.0.1"
      addr.to_s.should eq "127.0.0.1:1234"
    end

    it "preserves plain IPv4 addresses" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("10.1.2.3", 1234))

      addr.address.should eq "10.1.2.3"
    end

    it "preserves native IPv6 addresses" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::1", 1234))

      addr.address.should eq "::1"
    end
  end

  describe "#loopback?" do
    it "recognizes IPv4-mapped IPv6 loopback as loopback" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:127.0.0.1", 0))
      addr.loopback?.should be_true
    end
  end
end
