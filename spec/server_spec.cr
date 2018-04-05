require "./spec_helper"
require "amqp"
require "file_utils"

describe AvalancheMQ::Server do
  it "accepts connections" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5674) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5674, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("amq.topic", "topic", auto_delete: false, durable: true, internal: true, passive: true)
      q = ch.queue("", auto_delete: true, durable: false, exclusive: false)
      q.bind(x, "#")
      pmsg = AMQP::Message.new("test message")
      x.publish pmsg, q.name
      msg = q.get(no_ack: true)
      msg.to_s.should eq("test message")
    end
  ensure
    s.try &.close
  end

  it "can delete queue" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
    s.try &.close
  end

  it "can reject message" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("", auto_delete: true, durable: false, exclusive: false)
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject }
      m1 = q.get(no_ack: false)
      m1.should eq(nil)
    end
  ensure
    s.try &.close
  end

  it "can reject and requeue message" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
    s.try &.close
  end

  it "rejects all unacked msgs when disconnecting" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
    end
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      m1 = q.get(no_ack: true)
      m1.to_s.should eq("m1")
    end
  ensure
    s.try &.close
  end

  it "respects prefetch" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      ch.qos(0, 2, false)
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("", auto_delete: true, durable: false, exclusive: false)
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq(2)
    end
  ensure
    s.try &.close
  end

  it "respects prefetch and acks" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
    s.try &.close
  end

  it "can delete exchange" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x = ch.exchange("test_delete_exchange", "topic", durable: true)
      x.delete.should be x
    end
  ensure
    s.try &.close
  end

  it "can purge a queue" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", durable: true)
      q = ch.queue("test_purge", auto_delete: false, durable: true, exclusive: false)
      4.times { x.publish pmsg, q.name }
      q.purge.should eq 4
    end
  ensure
    s.try &.close
  end

  it "supports publisher confirms" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
    s.try &.close
  end

  it "supports mandatory publish flag" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      ch1 = Channel(Tuple(UInt16, String)).new
      ch.on_return do |code, text|
        ch1.send({ code, text })
      end
      ch.publish(pmsg, "amq.direct", "none", mandatory: true)
      reply_code, reply_text = ch1.receive
      reply_code.should eq 312
      reply_text.should eq "No Route"
    end
  ensure
    s.try &.close
  end

  it "expires messages" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
    s.try &.close
  end

  it "expires messages with message TTL on queue declaration" do
    s = AvalancheMQ::Server.new("/tmp/spec0", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
    s.try &.close
  end

  it "dead-letter expired messages" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
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
  ensure
     s.try &.close
  end

  it "handle immediate flag" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      reply_code = 0
      reply_msg = nil
      ch.on_return do |code, text|
        reply_code = code
        reply_msg = text
      end
      ch.publish(pmsg, "amq.topic", "rk", mandatory = false, immediate = true)
      wait_for { reply_code == 313 }
      reply_code.should eq 313
    end
  ensure
     s.try &.close
  end

  it "can cancel consumers" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.try &.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      msgs = [] of AMQP::Message
      tag = q.subscribe { |msg| msgs << msg }
      q.unsubscribe(tag)
      Fiber.yield
      ch.has_subscriber?(tag).should eq false
    end
  ensure
     s.try &.close
  end

  it "supports header exchange all" do
    s = AvalancheMQ::Server.new("/tmp/spec_qhe", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      q = ch.queue("q-header-ex", auto_delete: true, durable: false, exclusive: false)
      hdrs = AMQP::Protocol::Table.new
      hdrs["x-match"] = "all"
      hdrs["org"] = "84codes"
      hdrs["user"] = "test"
      x = ch.exchange("asdasdasd", "headers", passive: false, args: hdrs)
      q.bind(x)
      pmsg1 = AMQP::Message.new("m1", AMQP::Protocol::Properties.new(headers: hdrs))
      x.publish pmsg1, q.name
      hdrs["user"] = "hest"
      pmsg2 = AMQP::Message.new("m2", AMQP::Protocol::Properties.new(headers: hdrs))
      x.publish pmsg2, q.name
      msgs = [] of AMQP::Message
      tag = q.subscribe { |msg| msgs << msg }
      until msgs.size == 1
        Fiber.yield
      end
      msgs.size.should eq 1
    end
  ensure
    s.try &.close
  end

  it "supports header exchange any" do
    s = AvalancheMQ::Server.new("/tmp/spec_qhe2", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      q = ch.queue("q-header-ex2", auto_delete: true, durable: false, exclusive: false)
      hdrs = AMQP::Protocol::Table.new
      hdrs["x-match"] = "any"
      hdrs["org"] = "84codes"
      hdrs["user"] = "test"
      x = ch.exchange("", "headers", passive: false, args: hdrs)
      q.bind(x)
      pmsg1 = AMQP::Message.new("m1", AMQP::Protocol::Properties.new(headers: hdrs))
      x.publish pmsg1, q.name
      hdrs["user"] = "hest"
      pmsg2 = AMQP::Message.new("m2", AMQP::Protocol::Properties.new(headers: hdrs))
      x.publish pmsg2, q.name
      msgs = [] of AMQP::Message
      tag = q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq 2
    end
  ensure
    s.try &.close
  end

  it "splits frames into max frame sizes" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default", frame_max: 4096_u32)) do |conn|
      ch = conn.channel
      pmsg1 = AMQP::Message.new("m" * (2**17 + 1))
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("", auto_delete: true, durable: false, exclusive: false)
      x.publish pmsg1, q.name
      msgs = [] of AMQP::Message
      tag = q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq 1
    end
  ensure
    s.try &.close
  end

  it "acking an invalid delivery tag should close the channel" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      cch = Channel(Tuple(UInt16, String)).new
      ch.on_close do |code, text|
        cch.send({ code, text })
      end
      ch.ack(999_u64)
      code, text = cch.receive
      code.should eq 406
    end
  ensure
    s.try &.close
  end

  it "can bind exchanges to exchanges" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      x1 = ch.exchange("x1", "direct")
      x2 = ch.exchange("x2", "direct")
      x2.bind(x1, "e2e")
      q = ch.queue("e2e", auto_delete: true, durable: false, exclusive: false)
      q.bind(x2, "e2e")
      pmsg = AMQP::Message.new("test message")
      x1.publish pmsg, "e2e"
      Fiber.yield
      msg = q.get(no_ack: true)
      msg.to_s.should eq("test message")
    end
  ensure
    s.try &.close
  end

  it "supports x-max-length drop-head" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      ch.confirm
      acks = 0
      ch.on_confirm do |tag, acked|
        acks += 1 if acked
      end
      args = AMQP::Protocol::Table.new
      args["x-max-length"] = 1.to_u16
      q = ch.queue("", args: args)
      x = ch.exchange("", "direct")
      pmsg1 = AMQP::Message.new("m1")
      x.publish pmsg1, q.name
      pmsg2 = AMQP::Message.new("m2")
      x.publish pmsg2, q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      until msgs.size == 1
        Fiber.yield
      end
      acks.should eq 2
      msgs.size.should eq 1
    end
  ensure
    s.try &.close
  end

  it "supports x-max-length reject-publish" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start(AMQP::Config.new(host: "127.0.0.1", port: 5672, vhost: "default")) do |conn|
      ch = conn.channel
      ch.confirm
      acks = 0
      nacks = 0
      ch.on_confirm do |tag, acked|
        if acked
          acks += 1
        else
          nacks += 1
        end
      end
      args = AMQP::Protocol::Table.new
      args["x-max-length"] = 1.to_u16
      args["x-overflow"] = "reject-publish"
      q = ch.queue("", args: args)
      x = ch.exchange("", "direct")
      pmsg1 = AMQP::Message.new("m1")
      x.publish pmsg1, q.name
      pmsg2 = AMQP::Message.new("m2")
      x.publish pmsg2, q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      until msgs.size == 1
        Fiber.yield
      end
      acks.should eq 1
      nacks.should eq 1
      msgs.size.should eq 1
    end
  ensure
    s.try &.close
  end

  it "disallows creating queues starting with amq." do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      ch = conn.channel
      expect_raises(AMQP::ChannelClosed, /REFUSED/) do
        ch.queue("amq.test")
      end
    end
  ensure
    s.try &.close
  end

  it "disallows deleting exchanges named amq.*" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      ch = conn.channel
      expect_raises(AMQP::ChannelClosed, /REFUSED/) do
        ch.exchange("amq.topic", "topic", passive: true).delete
      end
      ch = conn.channel
      ch.exchange("amq.topic", "topic", passive: true).should_not be_nil
    end
  ensure
    s.try &.close
  end

  it "disallows creating new exchanges named amq.*" do
    s = AvalancheMQ::Server.new("/tmp/spec", Logger::ERROR)
    spawn { s.not_nil!.listen(5672) }
    Fiber.yield
    AMQP::Connection.start do |conn|
      ch = conn.channel
      expect_raises(AMQP::ChannelClosed, /REFUSED/) do
        ch.exchange("amq.topic2", "topic")
      end
    end
  ensure
    s.try &.close
  end
end
