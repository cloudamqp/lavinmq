require "../spec_helper"
require "../../src/lavinmq/clustering/proxy"

private AMQP_START = "AMQP\u0000\u0000\t\u0001"

# Sends *payload* through a proxy to a fake leader, returns what the leader got
private def forward_through_proxy(payload : String)
  leader = TCPServer.new("127.0.0.1", 0)
  proxy = LavinMQ::Clustering::Proxy.new("127.0.0.1", 0)
  proxy_port = proxy.@server.as(TCPServer).local_address.port
  spawn proxy.forward_to("127.0.0.1", leader.local_address.port, true)
  client = TCPSocket.new("127.0.0.1", proxy_port)
  client.write payload.to_slice
  upstream = leader.accept
  upstream.read_timeout = 5.seconds
  header = upstream.gets(chomp: false).should_not be_nil
  rest = Bytes.new(AMQP_START.bytesize)
  upstream.read_fully(rest)
  {header, String.new(rest), client.local_address}
ensure
  client.try &.close rescue nil
  upstream.try &.close rescue nil
  proxy.try &.close rescue nil
  leader.try &.close rescue nil
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
      header, rest, _ = forward_through_proxy("PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n#{AMQP_START}")
      header.should eq "PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n"
      rest.should eq AMQP_START
    end

    it "accepts a PROXY v2 header and forwards a single v1 header" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      io = IO::Memory.new
      src = Socket::IPAddress.new("203.0.113.7", 40000)
      dst = Socket::IPAddress.new("10.0.0.1", 5672)
      io.write_bytes LavinMQ::ProxyProtocol::V2.new(src, dst), IO::ByteFormat::NetworkEndian
      header, rest, _ = forward_through_proxy(String.new(io.to_slice) + AMQP_START)
      header.should eq "PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n"
      rest.should eq AMQP_START
    end

    it "strips the PROXY header of an untrusted source" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      LavinMQ::Config.instance.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("10.9.9.9")
      header, rest, client_addr = forward_through_proxy("PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n#{AMQP_START}")
      header.should start_with "PROXY TCP4 127.0.0.1 127.0.0.1 #{client_addr.port} "
      rest.should eq AMQP_START
    end

    it "adds a header to a connection without one" do
      LavinMQ::Config.instance.tcp_proxy_protocol = true
      header, rest, client_addr = forward_through_proxy(AMQP_START)
      header.should start_with "PROXY TCP4 127.0.0.1 127.0.0.1 #{client_addr.port} "
      rest.should eq AMQP_START
    end
  end
end
