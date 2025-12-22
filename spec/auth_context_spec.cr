require "./spec_helper"
require "../src/lavinmq/auth/context"

describe LavinMQ::Auth::Context do
  describe "basic initialization" do
    it "stores username, password, and loopback flag" do
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice, loopback: true)
      ctx.username.should eq "user"
      ctx.password.should eq "pass".to_slice
      ctx.loopback?.should be_true
    end

    it "defaults to non-loopback" do
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice)
      ctx.loopback?.should be_false
    end
  end

  describe "initialization with Socket::Address" do
    it "detects loopback for Socket::IPAddress with 127.0.0.1" do
      addr = Socket::IPAddress.new("127.0.0.1", 5672)
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice, addr)
      ctx.loopback?.should be_true
    end

    it "detects loopback for Socket::IPAddress with ::1" do
      addr = Socket::IPAddress.new("::1", 5672)
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice, addr)
      ctx.loopback?.should be_true
    end

    it "detects non-loopback for Socket::IPAddress with non-loopback address" do
      addr = Socket::IPAddress.new("192.168.1.1", 5672)
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice, addr)
      ctx.loopback?.should be_false
    end

    it "detects loopback for Socket::UNIXAddress" do
      addr = Socket::UNIXAddress.new("/tmp/test.sock")
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice, addr)
      ctx.loopback?.should be_true
    end

    it "detects non-loopback for nil address" do
      ctx = LavinMQ::Auth::Context.new("user", "pass".to_slice, nil)
      ctx.loopback?.should be_false
    end
  end
end
