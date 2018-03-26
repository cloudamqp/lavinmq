require "./spec_helper"
require "amqp"

describe AvalancheMQ::Server do
  it "accepts connections" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5674) }
    Fiber.yield
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
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
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
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
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
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
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
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
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
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
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
      Fiber.yield
      msgs.size.should eq(2)
    end
    s.close
  end

  it "respects prefetch and acks" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      ch.qos(0, 1, false)
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("", auto_delete: true, durable: false, exclusive: false)
      4.times { x.publish pmsg, q.name }
      c = 0
      q.subscribe do |msg|
        c += 1
        msg.ack
      end
      until c == 4
        Fiber.yield
      end
      c.should eq(4)
    end
    s.close
  end

  it "can delete exchange" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("test_delete_exchange", "topic", durable: true)
      x.delete.should be x
    end
    s.close
  end

  it "can purge a queue" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", durable: true)
      q = ch.queue("test_purge", auto_delete: false, durable: true, exclusive: false)
      4.times { x.publish pmsg, q.name }
      q.purge.should eq 4
    end
    s.close
  end

  it "supports publisher confirms" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
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
      q = ch.queue("test_confirms", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      ch.confirm
      x.publish pmsg, q.name
      q.get(no_ack: true)
      acked.should eq true
      delivery_tag.should eq 2
    end
    s.close
  end

  it "supports mandatory publish flag" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      reply_code = 0
      reply_msg = nil
      ch.on_return do |code, text|
        reply_code = code
        reply_msg = text
      end
      ch.publish(pmsg, "amq.topic", "rk", mandatory = true)
      until reply_code == 312
        Fiber.yield
      end
      reply_code.should eq 312
      reply_msg.should eq "No Route"
    end
    s.close
  end

  it "expires messages" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      q = ch.queue("exp1")

      x = ch.exchange("", "direct", passive: true)
      msg = AMQP::Message.new("expired",
                              AMQP::Protocol::Properties.new(expiration: "0"))
      x.publish msg, q.name

      sleep 0.01
      msg = q.get(no_ack: true)
      msg.to_s.should be ""
    end
    s.close
  end

  it "expires messages with message TTL on queue declaration" do
    s = AvalancheMQ::Server.new("/tmp/spec0", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      args = AMQP::Protocol::Table.new
      args["x-message-ttl"] = 1.to_u16
      args["x-dead-letter-exchange"] = ""
      args["x-dead-letter-routing-key"] = "dlq"
      q = ch.queue("", args: args)
      dlq = ch.queue("dlq")
      msg = AMQP::Message.new("queue dlx")
      x.publish msg, q.name
      sleep 0.01
      msg = dlq.get(no_ack: true)
      msg.to_s.should eq("queue dlx")
    end
    s.close
  end

  it "dead-letter expired messages" do
    s = AvalancheMQ::Server.new("/tmp/spec1", Logger::ERROR)
    spawn { s.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      dlq = ch.queue("dlq2")
      expq = ch.queue("exp")

      hdrs = AMQP::Protocol::Table.new
      hdrs["x-dead-letter-exchange"] = ""
      hdrs["x-dead-letter-routing-key"] = dlq.name
      msg = AMQP::Message.new("dead letter", AMQP::Protocol::Properties.new(expiration: "0", headers: hdrs))
      ch.exchange("", "direct", passive: true).publish msg, "exp"

      msgs = [] of AMQP::Message
      dlq.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq 1
      msgs.first.to_s.should eq("dead letter")
    end
    s.close
  end
end
