require "./spec_helper"

describe LavinMQ::ConnectionInfo::IPAddress do
  describe "#loopback?" do
    it "returns true for IPv4 loopback" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("127.0.0.1", 0))
      addr.loopback?.should be_true
    end

    it "returns true for IPv4 loopback subnet" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("127.0.0.2", 0))
      addr.loopback?.should be_true
    end

    it "returns true for IPv6 loopback" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::1", 0))
      addr.loopback?.should be_true
    end

    it "returns true for IPv4-mapped IPv6 loopback" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:127.0.0.1", 0))
      addr.loopback?.should be_true
    end

    it "returns false for non-loopback IPv4" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("192.168.1.1", 0))
      addr.loopback?.should be_false
    end

    it "returns false for non-loopback IPv6" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::2", 0))
      addr.loopback?.should be_false
    end

    it "returns false for non-loopback IPv4-mapped IPv6" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:192.168.1.1", 0))
      addr.loopback?.should be_false
    end
  end
end
