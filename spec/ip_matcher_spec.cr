require "./spec_helper"

describe LavinMQ::IPMatcher do
  describe ".parse and #matches?" do
    describe "IPv4 exact IP matching" do
      it "matches exact IPv4 address" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.1")
        matcher.matches?("192.168.1.1").should be_true
      end

      it "doesn't match different IPv4 address" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.1")
        matcher.matches?("192.168.1.2").should be_false
        matcher.matches?("192.168.2.1").should be_false
        matcher.matches?("10.0.0.1").should be_false
      end

      it "handles loopback address" do
        matcher = LavinMQ::IPMatcher.parse("127.0.0.1")
        matcher.matches?("127.0.0.1").should be_true
        matcher.matches?("127.0.0.2").should be_false
      end
    end

    describe "IPv6 exact IP matching" do
      it "matches exact IPv6 address" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::1")
        matcher.matches?("2001:db8::1").should be_true
      end

      it "doesn't match different IPv6 address" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::1")
        matcher.matches?("2001:db8::2").should be_false
        matcher.matches?("2001:db9::1").should be_false
      end

      it "handles IPv6 loopback" do
        matcher = LavinMQ::IPMatcher.parse("::1")
        matcher.matches?("::1").should be_true
        matcher.matches?("::2").should be_false
      end

      it "exact match requires same string representation" do
        # Exact IPs use string comparison for performance
        # Different representations of the same IPv6 address won't match
        matcher = LavinMQ::IPMatcher.parse("2001:0db8:0000:0000:0000:0000:0000:0001")
        matcher.matches?("2001:0db8:0000:0000:0000:0000:0000:0001").should be_true
        matcher.matches?("2001:db8::1").should be_false

        # If you need to match different representations, use CIDR /128
        cidr_matcher = LavinMQ::IPMatcher.parse("2001:db8::1/128")
        cidr_matcher.matches?("2001:db8::1").should be_true
      end
    end

    describe "IPv4 CIDR matching" do
      it "matches IP in /24 range" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.0/24")
        matcher.matches?("192.168.1.0").should be_true
        matcher.matches?("192.168.1.1").should be_true
        matcher.matches?("192.168.1.50").should be_true
        matcher.matches?("192.168.1.255").should be_true
      end

      it "doesn't match IP outside /24 range" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.0/24")
        matcher.matches?("192.168.0.255").should be_false
        matcher.matches?("192.168.2.0").should be_false
        matcher.matches?("192.169.1.1").should be_false
        matcher.matches?("10.0.0.1").should be_false
      end

      it "matches IP in /16 range" do
        matcher = LavinMQ::IPMatcher.parse("192.168.0.0/16")
        matcher.matches?("192.168.0.0").should be_true
        matcher.matches?("192.168.1.1").should be_true
        matcher.matches?("192.168.255.255").should be_true
      end

      it "doesn't match IP outside /16 range" do
        matcher = LavinMQ::IPMatcher.parse("192.168.0.0/16")
        matcher.matches?("192.167.255.255").should be_false
        matcher.matches?("192.169.0.0").should be_false
        matcher.matches?("10.0.0.1").should be_false
      end

      it "matches IP in /8 range" do
        matcher = LavinMQ::IPMatcher.parse("10.0.0.0/8")
        matcher.matches?("10.0.0.0").should be_true
        matcher.matches?("10.1.2.3").should be_true
        matcher.matches?("10.255.255.255").should be_true
      end

      it "doesn't match IP outside /8 range" do
        matcher = LavinMQ::IPMatcher.parse("10.0.0.0/8")
        matcher.matches?("9.255.255.255").should be_false
        matcher.matches?("11.0.0.0").should be_false
        matcher.matches?("192.168.1.1").should be_false
      end

      it "matches IP in /32 range (single host)" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.100/32")
        matcher.matches?("192.168.1.100").should be_true
        matcher.matches?("192.168.1.99").should be_false
        matcher.matches?("192.168.1.101").should be_false
      end

      it "matches IP in /0 range (all IPs)" do
        matcher = LavinMQ::IPMatcher.parse("0.0.0.0/0")
        matcher.matches?("0.0.0.0").should be_true
        matcher.matches?("192.168.1.1").should be_true
        matcher.matches?("255.255.255.255").should be_true
      end

      it "handles /25 CIDR" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.0/25")
        matcher.matches?("192.168.1.0").should be_true
        matcher.matches?("192.168.1.127").should be_true
        matcher.matches?("192.168.1.128").should be_false
        matcher.matches?("192.168.1.255").should be_false
      end

      it "handles /12 CIDR (AWS VPC default)" do
        matcher = LavinMQ::IPMatcher.parse("172.16.0.0/12")
        matcher.matches?("172.16.0.0").should be_true
        matcher.matches?("172.31.255.255").should be_true
        matcher.matches?("172.15.255.255").should be_false
        matcher.matches?("172.32.0.0").should be_false
      end

      it "normalizes network address" do
        # Even if network address isn't properly masked, it should work
        matcher = LavinMQ::IPMatcher.parse("192.168.1.50/24")
        matcher.matches?("192.168.1.1").should be_true
        matcher.matches?("192.168.1.255").should be_true
      end
    end

    describe "IPv6 CIDR matching" do
      it "matches IP in /64 range" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::/64")
        matcher.matches?("2001:db8::1").should be_true
        matcher.matches?("2001:db8::ffff").should be_true
        matcher.matches?("2001:db8:0:0:ffff:ffff:ffff:ffff").should be_true
      end

      it "doesn't match IP outside /64 range" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::/64")
        matcher.matches?("2001:db8:0:1::1").should be_false
        matcher.matches?("2001:db9::1").should be_false
        matcher.matches?("2001:db7:ffff:ffff:ffff:ffff:ffff:ffff").should be_false
      end

      it "matches IP in /32 range" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::/32")
        matcher.matches?("2001:db8::1").should be_true
        matcher.matches?("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff").should be_true
      end

      it "doesn't match IP outside /32 range" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::/32")
        matcher.matches?("2001:db7:ffff:ffff:ffff:ffff:ffff:ffff").should be_false
        matcher.matches?("2001:db9::1").should be_false
      end

      it "matches IP in /128 range (single host)" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::1/128")
        matcher.matches?("2001:db8::1").should be_true
        matcher.matches?("2001:db8::2").should be_false
      end

      it "matches IP in /0 range (all IPs)" do
        matcher = LavinMQ::IPMatcher.parse("::/0")
        matcher.matches?("::").should be_true
        matcher.matches?("2001:db8::1").should be_true
        matcher.matches?("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").should be_true
      end

      it "handles link-local /10 range" do
        matcher = LavinMQ::IPMatcher.parse("fe80::/10")
        matcher.matches?("fe80::1").should be_true
        matcher.matches?("fe80::ffff:ffff:ffff:ffff").should be_true
        matcher.matches?("febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff").should be_true
        matcher.matches?("fec0::1").should be_false
      end
    end

    describe "error handling" do
      it "raises on invalid IPv4 address" do
        expect_raises(ArgumentError, /Invalid IP address/) do
          LavinMQ::IPMatcher.parse("256.0.0.1")
        end

        expect_raises(ArgumentError, /Invalid IP address/) do
          LavinMQ::IPMatcher.parse("not-an-ip")
        end
      end

      it "raises on invalid IPv6 address" do
        expect_raises(ArgumentError, /Invalid IP address/) do
          LavinMQ::IPMatcher.parse("gggg::1")
        end
      end

      it "raises on invalid CIDR notation" do
        expect_raises(ArgumentError, /Invalid CIDR/) do
          LavinMQ::IPMatcher.parse("192.168.1.0//24")
        end

        expect_raises(ArgumentError, /Invalid CIDR/) do
          LavinMQ::IPMatcher.parse("192.168.1.0/")
        end
      end

      it "raises on invalid CIDR prefix" do
        expect_raises(ArgumentError, /Invalid CIDR prefix/) do
          LavinMQ::IPMatcher.parse("192.168.1.0/abc")
        end
      end

      it "raises on IPv4 prefix > 32" do
        expect_raises(ArgumentError, /IPv4 prefix must be 0-32/) do
          LavinMQ::IPMatcher.parse("192.168.1.0/33")
        end

        expect_raises(ArgumentError, /IPv4 prefix must be 0-32/) do
          LavinMQ::IPMatcher.parse("10.0.0.0/255")
        end
      end

      it "raises on IPv6 prefix > 128" do
        expect_raises(ArgumentError, /IPv6 prefix must be 0-128/) do
          LavinMQ::IPMatcher.parse("2001:db8::/129")
        end

        expect_raises(ArgumentError, /IPv6 prefix must be 0-128/) do
          LavinMQ::IPMatcher.parse("::1/255")
        end
      end

      it "raises on CIDR with invalid IP" do
        expect_raises(ArgumentError, /Invalid IP address in CIDR/) do
          LavinMQ::IPMatcher.parse("not-an-ip/24")
        end
      end
    end

    describe "edge cases" do
      it "handles whitespace in IP" do
        matcher = LavinMQ::IPMatcher.parse("  192.168.1.1  ")
        matcher.matches?("192.168.1.1").should be_true
      end

      it "handles whitespace in CIDR" do
        matcher = LavinMQ::IPMatcher.parse("  192.168.1.0  /  24  ")
        matcher.matches?("192.168.1.50").should be_true
      end

      it "doesn't match IPv6 address against IPv4 CIDR" do
        matcher = LavinMQ::IPMatcher.parse("192.168.1.0/24")
        matcher.matches?("2001:db8::1").should be_false
      end

      it "doesn't match IPv4 address against IPv6 CIDR" do
        matcher = LavinMQ::IPMatcher.parse("2001:db8::/32")
        matcher.matches?("192.168.1.1").should be_false
      end

      it "CIDR notation can match different IPv6 representations" do
        # CIDR notation uses byte comparison and can match different representations
        matcher = LavinMQ::IPMatcher.parse("2001:db8::1/128")
        matcher.matches?("2001:0db8:0000:0000:0000:0000:0000:0001").should be_true
        matcher.matches?("2001:db8::1").should be_true
      end
    end
  end
end
