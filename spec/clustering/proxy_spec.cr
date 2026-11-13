require "../spec_helper"
require "../../src/lavinmq/clustering/proxy"

private AMQP_START = "AMQP\u0000\u0000\t\u0001"

# Sends *payload* and the AMQP protocol header through a proxy to a fake
# leader, returns what the leader got in front of the AMQP protocol header
private def forward_through_proxy(payload : Bytes)
  leader = TCPServer.new("127.0.0.1", 0)
  proxy = LavinMQ::Clustering::Proxy.new("127.0.0.1", 0)
  proxy_port = proxy.@server.as(TCPServer).local_address.port
  spawn proxy.forward_to("127.0.0.1", leader.local_address.port, true)
  client = TCPSocket.new("127.0.0.1", proxy_port)
  client.write payload
  client.write AMQP_START.to_slice
  upstream = leader.accept
  upstream.read_timeout = 5.seconds
  received = IO::Memory.new
  buf = Bytes.new(256)
  until received.size >= AMQP_START.bytesize && received.to_slice[-AMQP_START.bytesize..] == AMQP_START.to_slice
    n = upstream.read(buf)
    raise "Leader connection closed" if n.zero?
    received.write buf[0, n]
  end
  header = received.to_slice[0, received.size - AMQP_START.bytesize]
  {header, client.local_address}
ensure
  client.try &.close rescue nil
  upstream.try &.close rescue nil
  proxy.try &.close rescue nil
  leader.try &.close rescue nil
end

# A PROXY v2 header with an SSL TLV, which the proxy's own v1 header can't carry
private def proxy_v2_header_with_tlv : Bytes
  version = "TLSv1.3"
  ssl_tlv = IO::Memory.new
  ssl_tlv.write_byte 0x01u8                               # client: SSL
  ssl_tlv.write_bytes 0u32, IO::ByteFormat::NetworkEndian # verified
  ssl_tlv.write_byte 0x21u8                               # sub type: VERSION
  ssl_tlv.write_bytes version.bytesize.to_u16, IO::ByteFormat::NetworkEndian
  ssl_tlv.write version.to_slice
  io = IO::Memory.new
  io.write LavinMQ::ProxyProtocol::V2::Signature.to_slice
  io.write_byte 0x21u8 # v2, PROXY
  io.write_byte 0x11u8 # TCP over IPv4
  io.write_bytes (12 + 3 + ssl_tlv.size).to_u16, IO::ByteFormat::NetworkEndian
  io.write Bytes[203, 0, 113, 7, 10, 0, 0, 1]
  io.write_bytes 40000u16, IO::ByteFormat::NetworkEndian
  io.write_bytes 5672u16, IO::ByteFormat::NetworkEndian
  io.write_byte 0x20u8 # TLV type: SSL
  io.write_bytes ssl_tlv.size.to_u16, IO::ByteFormat::NetworkEndian
  io.write ssl_tlv.to_slice
  io.to_slice
end

describe LavinMQ::Clustering::Proxy do
  it "closes the connection of a client that stops reading for tcp_send_timeout" do
    LavinMQ::Config.instance.tcp_send_timeout = 1
    leader = TCPServer.new("127.0.0.1", 0)
    proxy = LavinMQ::Clustering::Proxy.new("127.0.0.1", 0)
    proxy_port = proxy.@server.as(TCPServer).local_address.port
    spawn proxy.forward_to("127.0.0.1", leader.local_address.port)
    client = TCPSocket.new("127.0.0.1", proxy_port)
    upstream = leader.accept
    # The leader keeps sending to a client that never reads, until the proxy
    # gives up on the client and closes the connection to the leader too
    closed = Channel(Exception).new(1)
    spawn do
      chunk = Bytes.new(64 * 1024)
      loop { upstream.write chunk }
    rescue ex
      closed.send ex
    end
    select
    when ex = closed.receive
      ex.should be_a(IO::Error)
    when timeout(10.seconds)
      fail "The proxy kept the connection of a client that doesn't read"
    end
  ensure
    client.try &.close rescue nil
    upstream.try &.close rescue nil
    proxy.try &.close rescue nil
    leader.try &.close rescue nil
  end

  describe "with tcp_proxy_protocol" do
    it "replaces the PROXY header of a trusted source with one header for the original client" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      LavinMQ::Config.instance.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("127.0.0.1")
      header, _ = forward_through_proxy("PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n".to_slice)
      String.new(header).should eq "PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n"
    end

    it "forwards the PROXY v2 header of a trusted source unchanged, TLVs included" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      v2_header = proxy_v2_header_with_tlv
      header, _ = forward_through_proxy(v2_header)
      header.should eq v2_header
    end

    it "strips the PROXY header of an untrusted source" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      LavinMQ::Config.instance.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("10.9.9.9")
      header, client_addr = forward_through_proxy("PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n".to_slice)
      String.new(header).should match /\APROXY TCP4 127\.0\.0\.1 127\.0\.0\.1 #{client_addr.port} \d+\r\n\z/
    end

    it "strips the PROXY v2 header of an untrusted source" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      LavinMQ::Config.instance.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("10.9.9.9")
      header, client_addr = forward_through_proxy(proxy_v2_header_with_tlv)
      String.new(header).should match /\APROXY TCP4 127\.0\.0\.1 127\.0\.0\.1 #{client_addr.port} \d+\r\n\z/
    end

    it "adds a header to a connection without one" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      header, client_addr = forward_through_proxy(Bytes.empty)
      String.new(header).should match /\APROXY TCP4 127\.0\.0\.1 127\.0\.0\.1 #{client_addr.port} \d+\r\n\z/
    end
  end
end
