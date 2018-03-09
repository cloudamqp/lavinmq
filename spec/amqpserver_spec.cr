require "./spec_helper"
require "amqp"

describe AMQPServer::Server do
  it "accepts connections" do
    s = AMQPServer::Server.new("/tmp/spec3", Logger::DEBUG)
    spawn { s.listen(5674) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "/")) do |conn|
      ch = conn.channel
      x = ch.exchange("amq.topic", "topic", auto_delete: false, durable: true, internal: true, passive: true)
      q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
      q.bind(x, "#")
      pmsg = AMQP::Message.new("test message")
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      sleep 0.001
      msg = q.get(no_ack: true)
      msg.to_s.should eq("test message")
    end
    s.close
  end
end
