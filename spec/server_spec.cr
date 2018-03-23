require "./spec_helper"
require "amqp"

describe AvalancheMQ::Server do
  it "accepts connections" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
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
    s = AvalancheMQ::Server.new("/tmp/spec2", Logger::ERROR)
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
    s = AvalancheMQ::Server.new("/tmp/spec3", Logger::ERROR)
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
    s = AvalancheMQ::Server.new("/tmp/spec3", Logger::ERROR)
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
    s = AvalancheMQ::Server.new("/tmp/spec4", Logger::ERROR)
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

  it "respects prefetch" do
    s = AvalancheMQ::Server.new("/tmp/spec4", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      ch.qos(0, 2, false)
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      sleep 0.01
      msgs.size.should eq(2)
    end
    s.close
  end

  it "respects prefetch and acks" do
    s = AvalancheMQ::Server.new("/tmp/spec4", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      ch.qos(0, 1, false)
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("", auto_delete: false, durable: true, exclusive: false)
      4.times { x.publish pmsg, q.name }
      c = 0
      q.subscribe do |msg|
        c += 1
        msg.ack
      end
      until c == 4
        sleep 0.0001
      end
      c.should eq(4)
    end
    s.close
  end

  it "can delete exchange" do
    s = AvalancheMQ::Server.new("/tmp/spec5", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("test", "topic", durable: true)
      x.delete.should be x
    end
    s.close
  end

  it "can purge a queue" do
    s = AvalancheMQ::Server.new("/tmp/spec6", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", durable: true)
      q = ch.queue("test", auto_delete: false, durable: true, exclusive: false)
      4.times { x.publish pmsg, q.name }
      q.purge.should eq 4
    end
    s.close
  end

  it "supports publisher confirms" do
    s = AvalancheMQ::Server.new("/tmp/spec8", Logger::ERROR)
    spawn { s.listen(5672) }
    sleep 0.001
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      acked = false
      delivery_tag = 0
      ch.on_confirm do |tag, ack|
        delivery_tag = tag
        acked = ack
      end
      ch.confirm
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", durable: true)
      q = ch.queue("test", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      ch.confirm
      x.publish pmsg, q.name
      sleep 0.01
      acked.should eq true
      delivery_tag.should eq 2
    end
    s.close
  end
end
