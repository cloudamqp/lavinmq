require "./spec_helper"
require "amqp"

describe AMQPServer::Server do
  it "accepts connections" do
    s = AMQPServer::Server.new("/tmp", Logger::DEBUG)
    spawn { s.listen(5674) }
    sleep 1
    AMQP::Connection.start(AMQP::Config.new(port: 5674)) do |conn|
      ch1 = conn.channel
      ch2 = conn.channel
      q = ch2.queue("")
      x = ch1.exchange("", "direct", auto_delete: false)
      pmsg = AMQP::Message.new("test message")
      x.publish pmsg, q.name
      msg = q.get
      msg.to_s.should eq("test message")
    end
    s.close
  end
end
