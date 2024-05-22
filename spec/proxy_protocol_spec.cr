require "spec"
require "../src/lavinmq/proxy_protocol"

describe "ProxyProtocol" do
  describe "v1" do
    it "can parse valid data" do
      r, w = IO.pipe
      w.write "PROXY TCP 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice

      conn_info = LavinMQ::ProxyProtocol::V1.parse(r)
      conn_info.src.to_s.should eq "1.2.3.4:34567"
      conn_info.dst.to_s.should eq "127.0.0.2:1234"
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
      conn_info.src.to_s.should eq "127.0.0.1:37424"
      conn_info.dst.to_s.should eq "127.0.0.1:5671"
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
end
