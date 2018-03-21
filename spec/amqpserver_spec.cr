require "./spec_helper"
require "amqp"

describe AMQPServer::Server do
  it "accepts connections" do
    s = AMQPServer::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5674) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(port: 5674, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("amq.topic", "topic", auto_delete: false, durable: true, internal: true, passive: true)
      q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
      q.bind(x, "#")
      pmsg = AMQP::Message.new("test message")
      x.publish pmsg, q.name
      msg = q.get(no_ack: true)
      msg.to_s.should eq("test message")
    end
    s.close
  end

  it "can delete queue" do
    s = AMQPServer::Server.new("/tmp/spec2", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      q.delete
      ch.close

      ch = conn.channel
      pmsg = AMQP::Message.new("m2")
      q = ch.queue("q3", auto_delete: false, durable: true, exclusive: false)
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      msg = q.get
      msg.to_s.should eq("m2")
    end
    s.close
  end

  it "can reject message" do
    s = AMQPServer::Server.new("/tmp/spec3", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("q4", auto_delete: true, durable: false, exclusive: false)
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject }
      m1 = q.get(no_ack: false)
      m1.should eq(nil)
    end
    s.close
  end

  it "can reject and requeue message" do
    s = AMQPServer::Server.new("/tmp/spec3", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("q4", auto_delete: true, durable: false, exclusive: false)
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject(requeue: true) }
      m1 = q.get(no_ack: false)
      m1.to_s.should eq("m1")
    end
    s.close
  end

  it "rejects all unacked msgs when disconnecting" do
    s = AMQPServer::Server.new("/tmp/spec4", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
    end
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      m1 = q.get(no_ack: true)
      m1.to_s.should eq("m1")
    end
    s.close
  end
end
