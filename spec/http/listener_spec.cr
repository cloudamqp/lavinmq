require "../spec_helper"

describe LavinMQ::HTTP do
  describe ".tcp_listener" do
    it "closes the socket when binding it fails" do
      socket = nil
      expect_raises(Exception, "bind failed") do
        LavinMQ::HTTP.tcp_listener("127.0.0.1", 0) do |tcp|
          socket = tcp
          raise "bind failed"
        end
      end
      socket.try(&.closed?).should be_true
    end
  end

  describe "Server#close" do
    it "closes idle keep-alive connections it accepted" do
      with_amqp_server do |s|
        http = s.http_server
        listener = TCPServer.new("127.0.0.1", 0)
        client = TCPSocket.new("127.0.0.1", listener.local_address.port)
        accepted = listener.accept? || fail "nothing accepted"
        http.accept(accepted, nil)
        client << "GET /api/overview HTTP/1.1\r\nHost: x\r\n\r\n"
        client.flush
        response = ::HTTP::Client::Response.from_io(client)
        response.headers["Connection"]?.should_not eq "close"
        http.close
        client.read_timeout = 2.seconds
        client.read_byte.should be_nil # EOF, not waiting for the next request
      ensure
        client.try &.close
        listener.try &.close
      end
    end
  end
end
