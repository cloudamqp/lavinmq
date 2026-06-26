require "./spec_helper"
require "../src/lavinmq/proxy_protocol"

describe "ProxyProtocol" do
  describe "v1" do
    it "can parse valid data" do
      r, w = IO.pipe
      w.write "PROXY TCP 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice

      conn_info = LavinMQ::ProxyProtocol::V1.parse(r)
      conn_info.remote_address.to_s.should eq "1.2.3.4:34567"
      conn_info.local_address.to_s.should eq "127.0.0.2:1234"
      conn_info.ssl?.should be_false
    end

    it "can handle invalid data" do
      r, w = IO.pipe
      w.write "GET / HTTP/1.1\r\n".to_slice
      expect_raises(LavinMQ::ProxyProtocol::InvalidSignature) do
        LavinMQ::ProxyProtocol::V1.parse(r)
      end
    end

    it "can write a header for TCP4 addresses" do
      io = IO::Memory.new
      src = Socket::IPAddress.new("1.1.1.1", 80)
      dst = Socket::IPAddress.new("2.2.2.2", 8080)
      io.write_bytes LavinMQ::ProxyProtocol::V1.new(src, dst)
      io.to_s.should eq "PROXY TCP4 1.1.1.1 2.2.2.2 80 8080\r\n"
    end

    it "can write a header for TCPv6 addresses" do
      io = IO::Memory.new
      src = Socket::IPAddress.new("::1", 80)
      dst = Socket::IPAddress.new("::2", 8080)
      io.write_bytes LavinMQ::ProxyProtocol::V1.new(src, dst)
      io.to_s.should eq "PROXY TCP6 ::1 ::2 80 8080\r\n"
    end
  end

  describe "v2" do
    it "can parse valid data" do
      r, w = IO.pipe
      pp_bytes = UInt8.static_array(
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51,
        0x55, 0x49, 0x54, 0x0a, 0x21, 0x11, 0x00, 0x4e,
        0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01,
        0x92, 0x30, 0x16, 0x27, 0x20, 0x00, 0x3f, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x21, 0x00, 0x07, 0x54,
        0x4c, 0x53, 0x76, 0x31, 0x2e, 0x33, 0x25, 0x00,
        0x07, 0x52, 0x53, 0x41, 0x32, 0x30, 0x34, 0x38,
        0x24, 0x00, 0x0a, 0x52, 0x53, 0x41, 0x2d, 0x53,
        0x48, 0x41, 0x32, 0x35, 0x36, 0x23, 0x00, 0x16,
        0x54, 0x4c, 0x53, 0x5f, 0x41, 0x45, 0x53, 0x5f,
        0x32, 0x35, 0x36, 0x5f, 0x47, 0x43, 0x4d, 0x5f,
        0x53, 0x48, 0x41, 0x33, 0x38, 0x34, 0x41, 0x4d,
        0x51, 0x50, 0x00, 0x00, 0x09, 0x01
      )
      w.write pp_bytes.to_slice

      conn_info = LavinMQ::ProxyProtocol::V2.parse(r)
      conn_info.remote_address.to_s.should eq "127.0.0.1:37424"
      conn_info.local_address.to_s.should eq "127.0.0.1:5671"
      conn_info.ssl?.should be_true
      conn_info.ssl_version.should eq "TLSv1.3"
      conn_info.ssl_cipher.should eq "TLS_AES_256_GCM_SHA384"
    end

    it "can handle invalid data" do
      r, w = IO.pipe
      w.write "GET / HTTP/1.1\r\n".to_slice
      expect_raises(LavinMQ::ProxyProtocol::InvalidSignature) do
        LavinMQ::ProxyProtocol::V2.parse(r)
      end
    end

    it "can write header for TCPv4 addresses" do
      io = IO::Memory.new
      src = Socket::IPAddress.new("127.0.0.1", 37424)
      dst = Socket::IPAddress.new("127.0.0.1", 5671)
      io.write_bytes LavinMQ::ProxyProtocol::V2.new(src, dst), IO::ByteFormat::NetworkEndian
      io.to_slice.should eq UInt8.static_array(
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51,
        0x55, 0x49, 0x54, 0x0a, 0x21, 0x11, 0x00, 0x0c,
        0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01,
        0x92, 0x30, 0x16, 0x27
      ).to_slice
    end
  end

  describe "auto-detection" do
    it "auto-detects V1 protocol" do
      r, w = IO.pipe
      w.write "PROXY TCP4 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice

      conn_info = LavinMQ::ProxyProtocol.parse(r)
      conn_info.should_not be_nil
      conn_info.not_nil!.remote_address.to_s.should eq "1.2.3.4:34567"
      conn_info.not_nil!.local_address.to_s.should eq "127.0.0.2:1234"
    end

    it "auto-detects V2 protocol" do
      r, w = IO.pipe
      pp_bytes = UInt8.static_array(
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51,
        0x55, 0x49, 0x54, 0x0a, 0x21, 0x11, 0x00, 0x4e,
        0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01,
        0x92, 0x30, 0x16, 0x27, 0x20, 0x00, 0x3f, 0x01,
        0x00, 0x00, 0x00, 0x00, 0x21, 0x00, 0x07, 0x54,
        0x4c, 0x53, 0x76, 0x31, 0x2e, 0x33, 0x25, 0x00,
        0x07, 0x52, 0x53, 0x41, 0x32, 0x30, 0x34, 0x38,
        0x24, 0x00, 0x0a, 0x52, 0x53, 0x41, 0x2d, 0x53,
        0x48, 0x41, 0x32, 0x35, 0x36, 0x23, 0x00, 0x16,
        0x54, 0x4c, 0x53, 0x5f, 0x41, 0x45, 0x53, 0x5f,
        0x32, 0x35, 0x36, 0x5f, 0x47, 0x43, 0x4d, 0x5f,
        0x53, 0x48, 0x41, 0x33, 0x38, 0x34, 0x41, 0x4d,
        0x51, 0x50, 0x00, 0x00, 0x09, 0x01
      )
      w.write pp_bytes.to_slice

      conn_info = LavinMQ::ProxyProtocol.parse(r)
      conn_info.should_not be_nil
      conn_info.not_nil!.remote_address.to_s.should eq "127.0.0.1:37424"
      conn_info.not_nil!.ssl?.should be_true
    end

    it "returns nil for AMQP protocol header" do
      r, w = IO.pipe
      w.write "AMQP\x00\x00\x09\x01".to_slice

      conn_info = LavinMQ::ProxyProtocol.parse(r)
      conn_info.should be_nil
    end

    it "returns nil for MQTT protocol header" do
      r, w = IO.pipe
      w.write Bytes[0x10, 0x0e, 0x00, 0x04, 0x4d, 0x51, 0x54, 0x54]

      conn_info = LavinMQ::ProxyProtocol.parse(r)
      conn_info.should be_nil
    end

    it "returns nil for HTTP request" do
      r, w = IO.pipe
      w.write "GET / HTTP/1.1\r\n".to_slice

      conn_info = LavinMQ::ProxyProtocol.parse(r)
      conn_info.should be_nil
    end

    it "times out on the initial peek when the peer sends nothing" do
      r, _w = IO.pipe
      expect_raises(IO::TimeoutError) do
        LavinMQ::ProxyProtocol.parse(r, timeout: 100.milliseconds)
      end
      # read_timeout must be reset so it doesn't leak into the connection handler
      r.read_timeout.should be_nil
    end

    it "detects a V2 header split across TCP segments" do
      pp_bytes = UInt8.static_array(
        0x0d, 0x0a, 0x0d, 0x0a, 0x00, 0x0d, 0x0a, 0x51,
        0x55, 0x49, 0x54, 0x0a, 0x21, 0x11, 0x00, 0x0c,
        0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01,
        0x92, 0x30, 0x16, 0x27
      )
      r, w = IO.pipe
      ch = Channel(LavinMQ::ConnectionInfo?).new
      spawn { ch.send LavinMQ::ProxyProtocol.parse(r) }

      # First segment carries fewer than the 12 signature bytes
      w.write pp_bytes.to_slice[0, 6]
      Fiber.yield
      w.write pp_bytes.to_slice[6..]

      conn_info = ch.receive
      conn_info.should_not be_nil
      conn_info.not_nil!.remote_address.to_s.should eq "127.0.0.1:37424"
    end

    it "detects a V1 header split across TCP segments" do
      header = "PROXY TCP4 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice
      r, w = IO.pipe
      ch = Channel(LavinMQ::ConnectionInfo?).new
      spawn { ch.send LavinMQ::ProxyProtocol.parse(r) }

      w.write header[0, 3] # "PRO"
      Fiber.yield
      w.write header[3..]

      conn_info = ch.receive
      conn_info.should_not be_nil
      conn_info.not_nil!.remote_address.to_s.should eq "1.2.3.4:34567"
    end
  end

  describe "trusted sources" do
    it "parses individual IPv4 addresses" do
      config = LavinMQ::Config.new
      config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("192.168.1.1, 10.0.0.1")

      config.proxy_protocol_trusted_sources.size.should eq 2
      config.proxy_protocol_trusted_sources[0].matches?("192.168.1.1").should be_true
      config.proxy_protocol_trusted_sources[0].matches?("192.168.1.2").should be_false
    end

    it "parses IPv4 CIDR notation" do
      config = LavinMQ::Config.new
      config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("192.168.0.0/24")

      config.proxy_protocol_trusted_sources.size.should eq 1
      config.proxy_protocol_trusted_sources[0].matches?("192.168.0.1").should be_true
      config.proxy_protocol_trusted_sources[0].matches?("192.168.0.255").should be_true
      config.proxy_protocol_trusted_sources[0].matches?("192.168.1.1").should be_false
    end

    it "parses IPv6 CIDR notation" do
      config = LavinMQ::Config.new
      config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("2001:db8::/32")

      config.proxy_protocol_trusted_sources.size.should eq 1
      config.proxy_protocol_trusted_sources[0].matches?("2001:db8::1").should be_true
      config.proxy_protocol_trusted_sources[0].matches?("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff").should be_true
      config.proxy_protocol_trusted_sources[0].matches?("2001:db9::1").should be_false
    end

    it "parses mixed IPs and CIDR notation" do
      config = LavinMQ::Config.new
      config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list(
        "10.0.0.1, 192.168.0.0/24, 2001:db8::/32, ::1")

      config.proxy_protocol_trusted_sources.size.should eq 4

      # Exact IPv4
      config.proxy_protocol_trusted_sources[0].matches?("10.0.0.1").should be_true
      config.proxy_protocol_trusted_sources[0].matches?("10.0.0.2").should be_false

      # IPv4 CIDR
      config.proxy_protocol_trusted_sources[1].matches?("192.168.0.50").should be_true
      config.proxy_protocol_trusted_sources[1].matches?("192.168.1.50").should be_false

      # IPv6 CIDR
      config.proxy_protocol_trusted_sources[2].matches?("2001:db8::100").should be_true
      config.proxy_protocol_trusted_sources[2].matches?("2001:db9::1").should be_false

      # Exact IPv6
      config.proxy_protocol_trusted_sources[3].matches?("::1").should be_true
      config.proxy_protocol_trusted_sources[3].matches?("::2").should be_false
    end

    it "raises on an invalid entry mixed with valid ones" do
      # A typo must fail loudly rather than silently dropping the entry and
      # degrading the allow-list (potentially to trust-all).
      expect_raises(ArgumentError, /Invalid IP\/CIDR 'invalid-ip'/) do
        LavinMQ::IPMatcher.parse_list("10.0.0.1, invalid-ip, 192.168.0.0/24")
      end
    end

    it "raises when every entry is invalid instead of collapsing to trust-all" do
      # Regression for #2083: an all-invalid list must not parse to an empty
      # array (which trusted_proxy_source? treats as trust-all).
      expect_raises(ArgumentError, /Invalid IP\/CIDR/) do
        LavinMQ::IPMatcher.parse_list("10.0.0.0/33, 300.0.0.1")
      end
    end

    it "handles whitespace correctly" do
      config = LavinMQ::Config.new
      config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list(
        "  10.0.0.1  ,  192.168.0.0/24  ")

      config.proxy_protocol_trusted_sources.size.should eq 2
      config.proxy_protocol_trusted_sources[0].matches?("10.0.0.1").should be_true
      config.proxy_protocol_trusted_sources[1].matches?("192.168.0.50").should be_true
    end

    it "returns empty array for empty config" do
      config = LavinMQ::Config.new
      config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("")

      config.proxy_protocol_trusted_sources.size.should eq 0
    end

    it "fails loudly on an invalid entry (does not collapse to trust-all)" do
      expect_raises(ArgumentError, /Invalid IP\/CIDR/) do
        LavinMQ::IPMatcher.parse_list("10.0.0.0/33, not-an-ip")
      end
    end

    it "matches an IPv4 trusted source for an IPv4-mapped IPv6 peer (dual-stack)" do
      sources = LavinMQ::IPMatcher.parse_list("10.0.0.0/24")
      # An IPv4 load balancer behind a `bind = ::` listener arrives mapped
      sources.any?(&.matches?("::ffff:10.0.0.5")).should be_true
      sources.any?(&.matches?("::ffff:10.0.1.5")).should be_false
    end
  end
end
