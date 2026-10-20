require "log/spec"
require "./spec_helper"
require "../src/lavinmq/proxy_protocol"

private def join_bytes(*parts : Bytes) : Bytes
  io = IO::Memory.new
  parts.each { |part| io.write part }
  io.to_slice
end

private def tlv(type : UInt8, value : Bytes) : Bytes
  io = IO::Memory.new
  io.write_byte type
  io.write_bytes value.size.to_u16, IO::ByteFormat::NetworkEndian
  io.write value
  io.to_slice
end

private def ssl_tlv(client : UInt8, *sub_tlvs) : Bytes
  io = IO::Memory.new
  io.write_byte client
  io.write_bytes 0u32, IO::ByteFormat::NetworkEndian # verify
  sub_tlvs.each { |sub_tlv| io.write sub_tlv }
  tlv(0x20u8, io.to_slice)
end

# 127.0.0.1:37424 -> 127.0.0.1:5671
private PROXY_V2_TCPV4_ADDRESS = Bytes[0x7f, 0x00, 0x00, 0x01, 0x7f, 0x00, 0x00, 0x01, 0x92, 0x30, 0x16, 0x27]

private def proxy_v2_header(tlvs : Bytes = Bytes.empty, command = 0x21u8, family = 0x11u8,
                            address : Bytes = PROXY_V2_TCPV4_ADDRESS) : Bytes
  io = IO::Memory.new
  io.write LavinMQ::ProxyProtocol::V2::Signature.to_slice
  io.write_byte command
  io.write_byte family
  io.write_bytes (address.size + tlvs.size).to_u16, IO::ByteFormat::NetworkEndian
  io.write address
  io.write tlvs
  io.to_slice
end

# PROXY v2 header from 10.1.2.3:54321 to the given port
private def proxy_v2_header_from_client(port : Int32, tlvs : Bytes = Bytes.empty) : Bytes
  io = IO::Memory.new
  io.write Bytes[10, 1, 2, 3, 127, 0, 0, 1]
  io.write_bytes 54321u16, IO::ByteFormat::NetworkEndian
  io.write_bytes port.to_u16, IO::ByteFormat::NetworkEndian
  proxy_v2_header(tlvs, address: io.to_slice)
end

# Opens an AMQP connection as foo, with *proxy_header* sent first
private def amqp_open(port : Int32, proxy_header : Bytes) : TCPSocket
  socket = TCPSocket.new("localhost", port)
  socket.read_timeout = 5.seconds
  socket.write proxy_header
  socket.write LavinMQ::AMQP::PROTOCOL_START_0_9_1.to_slice
  socket.flush
  stream = AMQ::Protocol::Stream.new(socket)
  stream.next_frame.should be_a AMQ::Protocol::Frame::Connection::Start
  start_ok = AMQ::Protocol::Frame::Connection::StartOk.new(AMQ::Protocol::Table.new, "PLAIN", "\u0000foo\u0000bar", "en_US")
  socket.write_bytes start_ok, IO::ByteFormat::NetworkEndian
  socket.flush
  tune = stream.next_frame.as(AMQ::Protocol::Frame::Connection::Tune)
  socket.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(
    channel_max: tune.channel_max, frame_max: tune.frame_max, heartbeat: 0_u16),
    IO::ByteFormat::NetworkEndian
  socket.write_bytes AMQ::Protocol::Frame::Connection::Open.new("/"), IO::ByteFormat::NetworkEndian
  socket.flush
  stream.next_frame.should be_a AMQ::Protocol::Frame::Connection::OpenOk
  socket
end

private def connection_addresses(s : LavinMQ::Server) : Array(String)
  s.connections.compact_map(&.as?(LavinMQ::AMQP::Client)).map(&.connection_info.remote_address.to_s)
end

# Sends *proxy_header* to the AMQPS listener, then does the TLS handshake and
# gets Connection.Start, so the server has logged the connection's address
private def amqps_start(port : Int32, proxy_header : Bytes) : Nil
  socket = TCPSocket.new("localhost", port)
  socket.write proxy_header
  client_ctx = OpenSSL::SSL::Context::Client.new
  client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  tls = OpenSSL::SSL::Socket::Client.new(socket, client_ctx, sync_close: true)
  tls.write LavinMQ::AMQP::PROTOCOL_START_0_9_1.to_slice
  tls.flush
  tls.read_timeout = 5.seconds
  frame = Bytes.new(7)
  tls.read_fully(frame)
  frame[0].should eq 1u8 # Connection.Start method frame
ensure
  tls.try &.close rescue nil
  socket.try &.close rescue nil
end

describe "ProxyProtocol" do
  describe "v1" do
    it "can parse valid data" do
      r, w = IO.pipe
      w.write "PROXY TCP4 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice

      conn_info = LavinMQ::ProxyProtocol::V1.parse(r)
      conn_info.remote_address.to_s.should eq "1.2.3.4:34567"
      conn_info.local_address.to_s.should eq "127.0.0.2:1234"
      conn_info.ssl?.should be_false
      conn_info.proxied?.should be_true
    end

    it "raises for the UNKNOWN family so the connection is closed" do
      r, w = IO.pipe
      w.write "PROXY UNKNOWN\r\n".to_slice

      expect_raises(LavinMQ::ProxyProtocol::InvalidFamily) do
        LavinMQ::ProxyProtocol::V1.parse(r)
      end
    end

    it "raises for a family that is not TCP4 or TCP6" do
      r, w = IO.pipe
      w.write "PROXY TCP 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice

      expect_raises(LavinMQ::ProxyProtocol::InvalidFamily, "TCP") do
        LavinMQ::ProxyProtocol::V1.parse(r)
      end
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

      conn_info = LavinMQ::ProxyProtocol::V2.parse(r).should_not be_nil
      conn_info.remote_address.to_s.should eq "127.0.0.1:37424"
      conn_info.local_address.to_s.should eq "127.0.0.1:5671"
      conn_info.ssl?.should be_true
      conn_info.ssl_version.should eq "TLSv1.3"
      conn_info.ssl_cipher.should eq "TLS_AES_256_GCM_SHA384"
      conn_info.proxied?.should be_true
    end

    it "skips TLVs other than SSL, including unknown types" do
      r, w = IO.pipe
      w.write proxy_v2_header(join_bytes(
        tlv(0x02u8, "red-panda.example.com".to_slice),                           # AUTHORITY
        tlv(0x04u8, Bytes.new(3)),                                               # NOOP
        tlv(0x05u8, "unique-id".to_slice),                                       # UNIQUE_ID
        tlv(0xEAu8, join_bytes(Bytes[0x01], "vpce-0123456789abcdef0".to_slice)), # AWS VPC endpoint ID
        ssl_tlv(0x01u8, tlv(0x21u8, "TLSv1.3".to_slice)),
      ))

      conn_info = LavinMQ::ProxyProtocol::V2.parse(r).should_not be_nil
      conn_info.remote_address.to_s.should eq "127.0.0.1:37424"
      conn_info.local_address.to_s.should eq "127.0.0.1:5671"
      conn_info.ssl?.should be_true
      conn_info.ssl_version.should eq "TLSv1.3"
    end

    it "skips unknown SSL sub-TLVs" do
      r, w = IO.pipe
      w.write proxy_v2_header(ssl_tlv(0x01u8,
        tlv(0x26u8, "X25519".to_slice), # GROUP
        tlv(0x23u8, "TLS_AES_128_GCM_SHA256".to_slice),
        tlv(0x28u8, Bytes[0x30, 0x82]), # CLIENT_CERT
      ))

      conn_info = LavinMQ::ProxyProtocol::V2.parse(r).should_not be_nil
      conn_info.ssl_cipher.should eq "TLS_AES_128_GCM_SHA256"
    end

    it "reads the SSL TLV's client field as flags" do
      r, w = IO.pipe
      # SSL | CERT_CONN | CERT_SESS, sent when the client presented a certificate
      w.write proxy_v2_header(ssl_tlv(0x07u8, tlv(0x22u8, "client".to_slice)))

      conn_info = LavinMQ::ProxyProtocol::V2.parse(r).should_not be_nil
      conn_info.ssl?.should be_true
      conn_info.ssl_verify?.should be_true
      conn_info.ssl_cn.should eq "client"
    end

    it "doesn't mark the connection as TLS if the SSL TLV lacks the SSL client flag" do
      r, w = IO.pipe
      w.write proxy_v2_header(ssl_tlv(0x00u8))

      conn_info = LavinMQ::ProxyProtocol::V2.parse(r).should_not be_nil
      conn_info.ssl?.should be_false
    end

    it "returns nil for a LOCAL header and leaves the bytes after it" do
      r, w = IO.pipe
      w.write proxy_v2_header(command: 0x20u8, family: 0x00u8, address: Bytes.empty)
      w.write "AMQP".to_slice

      LavinMQ::ProxyProtocol::V2.parse(r).should be_nil
      buf = Bytes.new(4)
      r.read_fully(buf)
      String.new(buf).should eq "AMQP"
    end

    it "discards the address block of a LOCAL header" do
      r, w = IO.pipe
      w.write proxy_v2_header(tlv(0x02u8, "example.com".to_slice), command: 0x20u8)
      w.write "AMQP".to_slice

      LavinMQ::ProxyProtocol::V2.parse(r).should be_nil
      buf = Bytes.new(4)
      r.read_fully(buf)
      String.new(buf).should eq "AMQP"
    end

    it "raises for a PROXY header with an unspecified family, as the client address is unknown" do
      r, w = IO.pipe
      w.write proxy_v2_header(family: 0x00u8, address: Bytes.empty)

      expect_raises(LavinMQ::ProxyProtocol::InvalidFamily) do
        LavinMQ::ProxyProtocol::V2.parse(r)
      end
    end

    it "raises for an undefined family" do
      r, w = IO.pipe
      w.write proxy_v2_header(family: 0x41u8)

      expect_raises(LavinMQ::ProxyProtocol::InvalidFamily) do
        LavinMQ::ProxyProtocol::V2.parse(r)
      end
    end

    it "raises for a truncated address" do
      r, w = IO.pipe
      w.write proxy_v2_header(address: PROXY_V2_TCPV4_ADDRESS[0, 8])

      expect_raises(LavinMQ::ProxyProtocol::MalformedHeader) do
        LavinMQ::ProxyProtocol::V2.parse(r)
      end
    end

    it "raises for a TLV that is longer than the header" do
      r, w = IO.pipe
      w.write proxy_v2_header(Bytes[0x02u8, 0x00, 0x10, 0x61])

      expect_raises(LavinMQ::ProxyProtocol::MalformedHeader) do
        LavinMQ::ProxyProtocol::V2.parse(r)
      end
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

      conn_info = LavinMQ::ProxyProtocol.parse(r).should_not be_nil
      conn_info.remote_address.to_s.should eq "1.2.3.4:34567"
      conn_info.local_address.to_s.should eq "127.0.0.2:1234"
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

      conn_info = LavinMQ::ProxyProtocol.parse(r).should_not be_nil
      conn_info.remote_address.to_s.should eq "127.0.0.1:37424"
      conn_info.ssl?.should be_true
      conn_info.proxied?.should be_true
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

      conn_info = ch.receive.should_not be_nil
      conn_info.remote_address.to_s.should eq "127.0.0.1:37424"
    end

    it "detects a V1 header split across TCP segments" do
      header = "PROXY TCP4 1.2.3.4 127.0.0.2 34567 1234\r\n".to_slice
      r, w = IO.pipe
      ch = Channel(LavinMQ::ConnectionInfo?).new
      spawn { ch.send LavinMQ::ProxyProtocol.parse(r) }

      w.write header[0, 3] # "PRO"
      Fiber.yield
      w.write header[3..]

      conn_info = ch.receive.should_not be_nil
      conn_info.remote_address.to_s.should eq "1.2.3.4:34567"
    end

    it "detects a header without reading it" do
      r, w = IO.pipe
      w.write proxy_v2_header

      LavinMQ::ProxyProtocol.header?(r).should be_true
      LavinMQ::ProxyProtocol.parse(r).should_not be_nil
    end

    it "detects no header in front of AMQP" do
      r, w = IO.pipe
      w.write "AMQP\x00\x00\x09\x01".to_slice

      LavinMQ::ProxyProtocol.header?(r).should be_false
    end
  end

  describe "listeners" do
    it "accept a PROXY v2 header with TLVs other than SSL" do
      with_amqp_server do |s|
        with_proxy_protocol do
          s.users.create("foo", "bar")
          s.users.add_permission "foo", "/", /.*/, /.*/, /.*/
          port = amqp_port(s)
          header = proxy_v2_header_from_client(port, tlv(0x02u8, "red-panda.example.com".to_slice))
          socket = amqp_open(port, header)
          begin
            connection_addresses(s).should eq ["10.1.2.3:54321"]
          ensure
            socket.close
          end
        end
      end
    end

    it "accept a LOCAL header, e.g. from a health check, using the connection's own address" do
      with_amqp_server do |s|
        with_proxy_protocol do
          s.users.create("foo", "bar")
          s.users.add_permission "foo", "/", /.*/, /.*/, /.*/
          header = proxy_v2_header(command: 0x20u8, family: 0x00u8, address: Bytes.empty)
          socket = amqp_open(amqp_port(s), header)
          begin
            local = LavinMQ::ConnectionInfo::IPAddress.new(socket.local_address).to_s
            connection_addresses(s).should eq [local]
          ensure
            socket.close
          end
        end
      end
    end

    it "read a PROXY header in front of TLS from a trusted source" do
      with_amqp_server(tls: true) do |s|
        with_proxy_protocol do
          port = amqp_tls_port(s)
          Log.capture("lmq.server", :info) do |logs|
            amqps_start(port, proxy_v2_header_from_client(port))
            logs.check(:info, /^10\.1\.2\.3:54321 connected with /)
          end
        end
      end
    end

    it "ignore a PROXY header in front of TLS from an untrusted source" do
      with_amqp_server(tls: true) do |s|
        with_proxy_protocol([LavinMQ::IPMatcher.parse("10.0.0.1")]) do
          port = amqp_tls_port(s)
          Log.capture("lmq.server", :info) do |logs|
            amqps_start(port, proxy_v2_header_from_client(port))
            logs.check(:warn, /^PROXY protocol from untrusted source /)
            logs.check(:info, /^(127\.0\.0\.1|::1):\d+ connected with /)
          end
        end
      end
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
