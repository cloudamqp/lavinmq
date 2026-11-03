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
end
