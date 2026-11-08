require "../spec_helper"
require "../../src/lavinmq/clustering/proxy"

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
end
