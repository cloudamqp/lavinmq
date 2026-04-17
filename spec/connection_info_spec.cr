require "./spec_helper"

describe LavinMQ::ConnectionInfo::IPAddress do
  describe "#loopback?" do
    it "recognizes IPv4-mapped IPv6 loopback as loopback" do
      addr = LavinMQ::ConnectionInfo::IPAddress.new(Socket::IPAddress.new("::ffff:127.0.0.1", 0))
      addr.loopback?.should be_true
    end
  end
end
