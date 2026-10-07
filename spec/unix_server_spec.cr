require "./spec_helper"

describe LavinMQ::Server do
  describe "UNIX Sockets" do
    pending "can accept UNIX socket connections" do
      s = LavinMQ::Server.new(LavinMQ::Config.instance)
      amqp_server = s.amqp_server
      begin
        amqp_server.bind_unix("/tmp/lavinmq-spec/lavinmq.sock")
        spawn { amqp_server.listen }
        sleep 10.milliseconds
        with_channel(s, host: "/tmp/lavinmq-spec/lavinmq.sock") do |ch|
          ch.should_not be_nil
        end
      ensure
        s.close unless s.closed? # also closes the protocol server held by `s`
      end
    end
  end
end
