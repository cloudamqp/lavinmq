require "./spec_helper"

describe AvalancheMQ::Server do
  it "accepts connections" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      x = ch.exchange("amq.topic", "topic", auto_delete: false, durable: true, internal: true, passive: true)
      q = ch.queue("")
      q.bind(x, "#")
      pmsg = AMQP::Message.new("test message")
      x.publish pmsg, q.name
      msg = q.get(no_ack: true)
      msg.to_s.should eq("test message")
    end
  end

  it "can delete queue" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("")
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      q.delete
      ch.close

      ch = conn.channel
      pmsg = AMQP::Message.new("m2")
      q = ch.queue("")
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      msg = q.get
      msg.to_s.should eq("m2")
    end
  end

  it "can reject message" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("")
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject }
      m1 = q.get(no_ack: false)
      m1.should eq(nil)
    end
  end

  it "can reject and requeue message" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      q = ch.queue("")
      x = ch.exchange("", "direct", passive: true)
      x.publish pmsg, q.name
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject(requeue: true) }
      m1 = q.get(no_ack: false)
      m1.to_s.should eq("m1")
      m1.not_nil!.redelivered.should be_true
    end
  end

  it "rejects all unacked msgs when disconnecting" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      q.get(no_ack: false)
    end
    AMQP::Connection.start do |conn|
      ch = conn.channel
      ch.exchange("", "direct", passive: true)
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      m1 = q.get(no_ack: true)
      m1.to_s.should eq("m1")
    end
  ensure
    s.vhosts["/"].delete_queue("q5")
  end

  it "respects prefetch" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      ch.qos(0, 2, false)
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("")
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      x.publish pmsg, q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq(2)
    end
  end

  it "respects prefetch and acks" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      ch.qos(0, 1, false)
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("")
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
  end

  it "can delete exchange" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      x = ch.exchange("test_delete_exchange", "topic", durable: true)
      x.delete.should be x
    end
  ensure
    s.vhosts["/"].delete_exchange("test_delete_exchange")
  end

  it "can auto delete exchange" do
    AMQP::Connection.start do |conn|
      ch = conn.channel.confirm
      code = 0
      ch.on_close do |c, _reply|
        code = c
      end
      x = ch.exchange("test_ad_exchange", "topic", durable: false, auto_delete: true)
      q = ch.queue("")
      q.bind(x)
      q.unbind(x)
      x.publish(AMQP::Message.new("m1"), q.name)
      wait_for { code == 404 }
      ch.closed.should be_true
    end
  ensure
    s.vhosts["/"].delete_exchange("test_ad_exchange")
  end

  it "can purge a queue" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", durable: true)
      q = ch.queue("")
      4.times { x.publish pmsg, q.name }
      q.purge.should eq 4
    end
  end

  it "supports publisher confirms" do
    AMQP::Connection.start do |conn|
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
      q = ch.queue("")
      x.publish pmsg, q.name
      ch.confirm
      x.publish pmsg, q.name
      q.get(no_ack: true)
      acked.should eq true
      delivery_tag.should eq 2
    end
  end

  it "supports mandatory publish flag" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      ch1 = Channel(Tuple(UInt16, String)).new
      ch.on_return do |code, text|
        ch1.send({code, text})
      end
      ch.publish(pmsg, "amq.direct", "none", mandatory: true)
      reply_code, reply_text = ch1.receive
      reply_code.should eq 312
      reply_text.should eq "No Route"
    end
  end

  it "expires messages" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
      x = ch.exchange("", "direct", passive: true)
      msg = AMQP::Message.new("expired",
        AMQP::Protocol::Properties.new(expiration: "0"))
      x.publish msg, q.name
      sleep 0.05
      msg = q.get(no_ack: true)
      msg.to_s.should be ""
    end
  end

  it "expires messages with message TTL on queue declaration" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      x = ch.exchange("", "direct", passive: true)
      args = AMQP::Protocol::Table.new
      args["x-message-ttl"] = 1
      args["x-dead-letter-exchange"] = ""
      args["x-dead-letter-routing-key"] = "dlq"
      q = ch.queue("", args: args)
      dlq = ch.queue("dlq")
      msg = AMQP::Message.new("queue dlx")
      x.publish msg, q.name
      sleep 0.05
      msg = dlq.get(no_ack: true)
      msg.to_s.should eq("queue dlx")
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
  end

  it "dead-letter expired messages" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      dlq = ch.queue("")
      ch.queue("exp")

      hdrs = AMQP::Protocol::Table.new
      hdrs["x-dead-letter-exchange"] = ""
      hdrs["x-dead-letter-routing-key"] = dlq.name
      msg = AMQP::Message.new("dead letter", AMQP::Protocol::Properties.new(expiration: "0", headers: hdrs))
      ch.exchange("", "direct", passive: true).publish msg, "exp"

      msgs = [] of AMQP::Message
      dlq.subscribe { |m| msgs << m }
      Fiber.yield
      msgs.size.should eq 1
      msgs.first.to_s.should eq("dead letter")
    end
  end

  it "handle immediate flag" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      reply_code = 0
      reply_msg = nil
      ch.on_return do |code, text|
        reply_code = code
        reply_msg = text
      end
      ch.publish(pmsg, "amq.topic", "rk", mandatory: false, immediate: true)
      wait_for { reply_code == 313 }
      reply_code.should eq 313
    end
  end

  it "can cancel consumers" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg = AMQP::Message.new("m1")
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("", auto_delete: false, durable: true, exclusive: false)
      x.publish pmsg, q.name
      msgs = [] of AMQP::Message
      tag = q.subscribe { |msg| msgs << msg }
      q.unsubscribe(tag)
      Fiber.yield
      ch.has_subscriber?(tag).should eq false
    end
  end

  it "supports header exchange all" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
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
      q.subscribe { |msg| msgs << msg }
      until msgs.size == 1
        Fiber.yield
      end
      msgs.size.should eq 1
    end
  end

  it "supports header exchange any" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
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
      q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq 2
    end
  end

  it "splits frames into max frame sizes" do
    AMQP::Connection.start(AMQP::Config.new(port: 5672, frame_max: 4096_u32)) do |conn|
      ch = conn.channel
      pmsg1 = AMQP::Message.new("m" * (2**17 + 1))
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("")
      q.purge
      x.publish pmsg1, q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq 1
    end
  end

  it "can receive and deliver large messages" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      pmsg1 = AMQP::Message.new("a" * 8133)
      x = ch.exchange("", "direct", passive: true)
      q = ch.queue("")
      x.publish pmsg1, q.name
      msg = q.get
      msg.to_s.should eq pmsg1.to_s
    end
  end

  it "acking an invalid delivery tag should close the channel" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      cch = Channel(Tuple(UInt16, String)).new
      ch.on_close do |code, text|
        cch.send({code, text})
      end
      ch.ack(999_u64)
      code = cch.receive.first
      code.should eq 406
    end
  end

  it "can bind exchanges to exchanges" do
    AMQP::Connection.start do |conn|
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
    s.vhosts["/"].delete_exchange("x1")
    s.vhosts["/"].delete_exchange("x2")
    s.vhosts["/"].delete_queue("e2e")
  end

  it "supports x-max-length drop-head" do
    AMQP::Connection.start do |conn|
      ch = conn.channel.confirm
      acks = 0
      ch.on_confirm do |_tag, acked|
        acks += 1 if acked
      end
      args = AMQP::Protocol::Table.new
      args["x-max-length"] = 1_i64
      q = ch.queue("", args: args)
      x = ch.exchange("", "direct")
      x.publish AMQP::Message.new("m1"), q.name
      x.publish AMQP::Message.new("m2"), q.name
      sleep 0.05
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      acks.should eq 2
      msgs.size.should eq 1
    end
  end

  it "supports x-max-length reject-publish" do
    AMQP::Connection.start do |conn|
      ch = conn.channel.confirm
      acks = 0
      nacks = 0
      ch.on_confirm do |_tag, acked|
        acked ? (acks += 1) : (nacks += 1)
      end
      args = AMQP::Protocol::Table.new
      args["x-max-length"] = 1_i64
      args["x-overflow"] = "reject-publish"
      q = ch.queue("", args: args)
      x = ch.exchange("", "direct")
      x.publish AMQP::Message.new("m1"), q.name
      x.publish AMQP::Message.new("m2"), q.name
      sleep 0.05
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      acks.should eq 2
      nacks.should eq 0
      msgs.size.should eq 1
    end
  end

  it "disallows creating queues starting with amq." do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      expect_raises(AMQP::ChannelClosed, /REFUSED/) do
        ch.queue("amq.test")
      end
    end
  end

  it "disallows deleting exchanges named amq.*" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      expect_raises(AMQP::ChannelClosed, /REFUSED/) do
        ch.exchange("amq.topic", "topic", passive: true).delete
      end
      ch = conn.channel
      ch.exchange("amq.topic", "topic", passive: true).should_not be_nil
    end
  end

  it "disallows creating new exchanges named amq.*" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      expect_raises(AMQP::ChannelClosed, /REFUSED/) do
        ch.exchange("amq.topic2", "topic")
      end
    end
  end

  it "only allow one consumer on when exlusive consumers flag is set" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("exlusive_consumer", auto_delete: true)
      q.subscribe(exclusive: true) { }

      ch2 = conn.channel
      q2 = ch2.queue("exlusive_consumer", passive: true)
      expect_raises(AMQP::ChannelClosed, /ACCESS_REFUSED/) do
        q2.subscribe { }
      end
      ch.close
      ch = conn.channel
      q = ch.queue("exlusive_consumer", auto_delete: true)
      q.subscribe { }
    end
  ensure
    s.vhosts["/"].delete_queue("exlusive_consumer")
  end

  it "only allow one connection access an exlusive queues" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      ch.queue("exlusive_queue", durable: true, exclusive: true)
      AMQP::Connection.start do |conn2|
        ch2 = conn2.channel
        expect_raises(AMQP::ChannelClosed, /RESOURCE_LOCKED/) do
          ch2.queue("exlusive_queue", passive: true)
        end
      end
    end
  ensure
    s.vhosts["/"].delete_queue("exlusive_consumer")
  end

  it "it persists transient msgs between restarts" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("durable_queue", durable: true)
      x = ch.exchange("", "direct", passive: true)
      1000.times do |i|
        delivery_mode = i % 2 == 0 ? 2_u8 : 0_u8
        props = AMQP::Protocol::Properties.new(delivery_mode: delivery_mode)
        msg = AMQP::Message.new(i.to_s, props)
        x.publish(msg, q.name)
      end
    end
    wait_for { s.vhosts["/"].queues["durable_queue"].message_count == 1000 }
    close_servers
    TestHelpers.setup
    sleep 0.05
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("durable_queue", durable: true)
      deleted_msgs = q.delete
      deleted_msgs.should eq(1000)
    end
  ensure
    s.vhosts["/"].delete_queue("durable_queue")
  end

  it "supports max-length" do
    definitions = {"max-length" => JSON::Any.new(1_i64)}
    s.vhosts["/"].add_policy("ml", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 10_i8)
    AMQP::Connection.start do |conn|
      ch = conn.channel.confirm
      acks = 0
      nacks = 0
      ch.on_confirm do |_tag, acked|
        acked ? (acks += 1) : (nacks += 1)
      end

      q = ch.queue("")
      x = ch.exchange("", "direct")
      x.publish AMQP::Message.new("m1"), q.name
      x.publish AMQP::Message.new("m2"), q.name
      msgs = [] of AMQP::Message
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      acks.should eq 2
      msgs.size.should eq 1
    end
  ensure
    s.vhosts["/"].delete_policy("ml")
  end

  it "supports alternate-exchange" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      args = AMQP::Protocol::Table.new
      args["x-alternate-exchange"] = "ae"
      x1 = ch.exchange("x1", "topic", args: args)
      ae = ch.exchange("ae", "topic")
      q = ch.queue("")
      q.bind(ae, "*")
      x1.publish(AMQP::Message.new("m1"), "rk")
      msg = q.get(no_ack: true)
      msg.to_s.should eq("m1")
    end
  ensure
    s.vhosts["/"].delete_exchange("x1")
    s.vhosts["/"].delete_exchange("ae")
  end

  it "supports heartbeats" do
    s = AvalancheMQ::Server.new("/tmp/spec", LOG_LEVEL, config: {"heartbeat" => 1_u16})
    s.config["heartbeat"].should eq 1
  end

  it "supports expires" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      args = AMQP::Protocol::Table.new
      args["x-expires"] = 1
      ch.queue("test", args: args)
      sleep 5.milliseconds
      Fiber.yield
      s.vhosts["/"].queues.has_key?("test").should be_false
    end
  ensure
    s.vhosts["/"].delete_queue("test")
  end

  it "should deliver to all matching queues" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q1 = ch.queue("")
      q2 = ch.queue("")
      x1 = ch.exchange("x122", "topic")
      q1.bind(x1, "rk")
      q2.bind(x1, "rk")
      x1.publish(AMQP::Message.new("m1"), "rk")
      sleep 0.05
      msg_q1 = q1.get(no_ack: true)
      msg_q2 = q2.get(no_ack: true)
      msg_q1.to_s.should eq("m1")
      msg_q2.to_s.should eq("m1")
    end
  ensure
    s.vhosts["/"].delete_exchange("x122")
  end

  it "supports auto ack consumers" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
      x = ch.exchange("", "direct")
      x.publish AMQP::Message.new("m1"), q.name
      msgs = [] of AMQP::Message
      q.subscribe(no_ack: true) { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "sets correct message timestamp" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
      x = ch.exchange("", "direct")
      t = Time.utc_now.epoch
      x.publish AMQP::Message.new("m1"), q.name
      msg = nil
      q.subscribe(no_ack: true) { |m| msg = m }
      wait_for { msg }
      msg.not_nil!.properties.timestamp.epoch.should be_close(t, 1)
    end
  end

  it "supports recover requeue" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
      x = ch.exchange("", "direct")
      x.publish AMQP::Message.new("m1"), q.name
      delivered = 0
      q.subscribe(no_ack: false) { |m| delivered += 1 }
      ch.recover(requeue: true)
      Fiber.yield
      delivered.should eq 2
    end
  end

  it "supports recover redeliver" do
    AMQP::Connection.start do |conn|
      ch = conn.channel
      q = ch.queue("")
      x = ch.exchange("", "direct")
      x.publish AMQP::Message.new("m1"), q.name
      msg = nil
      q.subscribe(no_ack: false) { |m| msg = m }
      ch.recover(requeue: true)
      Fiber.yield
      msg.not_nil!.redelivered.should be_true
    end
  end
end
