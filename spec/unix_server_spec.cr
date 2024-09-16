require "./spec_helper"

describe LavinMQ::Server do
  describe "UNIX Sockets" do
    pending "can accept UNIX socket connections" do
      spawn { s.listen_unix("/tmp/lavinmq-spec/lavinmq.sock") }
      sleep 10.milliseconds
      with_channel(host: "/tmp/lavinmq-spec/lavinmq.sock") do |ch|
        ch.should_not be_nil
      end
    end
  end
end
