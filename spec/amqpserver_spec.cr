require "./spec_helper"
require "amqp"

describe AMQPServer::Server do
  it "accepts connections" do
    s = AMQPServer::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5674) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(port: 5674, vhost: "default")) do |conn|
      ch1 = conn.channel
      ch2 = conn.channel
      q = ch2.queue("", auto_delete: true, durable: false, exclusive: true)
      x = ch1.exchange("amq.direct", "direct", auto_delete: false, durable: true)
      pmsg = AMQP::Message.new("test message")
      x.publish pmsg, q.name
      msg = q.get
      msg.to_s.should eq("test message")
    end
    s.close
  end
end
