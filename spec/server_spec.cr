require "./spec_helper"

describe AvalancheMQ::Server do
  it "accepts connections" do
    with_channel do |ch|
      x = ch.exchange("amq.topic", "topic", auto_delete: false, durable: true, internal: true, passive: true)
      q = ch.queue
      q.bind(x.name, "#")
      pmsg = "test message"
      x.publish pmsg, q.name
      msg = q.get(no_ack: true).not_nil!
      msg.body_io.to_s.should eq("test message")
    end
  end

  it "can delete queue" do
    with_channel do |ch|
      q = ch.queue("del_q")
      q.publish "m1"
      q.delete
    end
    with_channel do |ch|
      pmsg = "m2"
      q = ch.queue("del_q")
      q.publish pmsg
      msg = q.get.not_nil!
      msg.body_io.to_s.should eq("m2")
    end
  ensure
    s.vhosts["/"].delete_queue("del_q")
  end

  it "can reject message" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject }
      m1 = q.get(no_ack: false)
      m1.should eq(nil)
    end
  end

  it "can reject and requeue message" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      m1 = q.get(no_ack: false)
      m1.try { |m| m.reject(requeue: true) }
      m1 = q.get(no_ack: false)
      m1.not_nil!.body_io.to_s.should eq("m1")
      m1.not_nil!.redelivered.should be_true
    end
  end

  it "rejects all unacked msgs when disconnecting" do
    with_channel do |ch|
      pmsg = "m1"
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      q.publish pmsg
      q.get(no_ack: false)
    end
    with_channel do |ch|
      q = ch.queue("q5", auto_delete: false, durable: true, exclusive: false)
      m1 = q.get(no_ack: true)
      m1.not_nil!.body_io.to_s.should eq("m1")
    end
  ensure
    s.vhosts["/"].delete_queue("q5")
  end

  it "respects prefetch" do
    with_channel do |ch|
      ch.prefetch 2
      pmsg = "m1"
      q = ch.queue
      q.publish pmsg
      q.publish pmsg
      q.publish pmsg
      msgs = [] of AMQP::Client::Message
      q.subscribe(no_ack: false) { |msg| msgs << msg }
      Fiber.yield
      Fiber.yield
      Fiber.yield
      msgs.size.should eq(2)
    end
  end

  it "respects prefetch and acks" do
    with_channel do |ch|
      ch.prefetch 1
      q = ch.queue
      4.times { q.publish "m1" }
      c = 0
      q.subscribe(no_ack: false) do |msg|
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
    with_channel do |ch|
      x = ch.exchange("test_delete_exchange", "topic", durable: true)
      x.delete
      expect_raises(AMQP::Client::Channel::ClosedException) do
        ch.exchange("test_delete_exchange", "topic", durable: true, passive: true)
      end
    end
  ensure
    s.vhosts["/"].delete_exchange("test_delete_exchange")
  end

  it "can auto delete exchange" do
    with_channel do |ch|
      ch.confirm_select
      code = 0
      ch.on_close do |c, _reply|
        code = c
      end
      x = ch.exchange("test_ad_exchange", "topic", durable: false, auto_delete: true)
      q = ch.queue
      q.bind(x.name, "")
      q.unbind(x.name, "")
      x.publish("m1", q.name)
      wait_for { code == 404 }
      ch.closed?.should be_true
    end
  ensure
    s.vhosts["/"].delete_exchange("test_ad_exchange")
  end

  it "can purge a queue" do
    with_channel do |ch|
      q = ch.queue
      4.times { q.publish "" }
      q.purge[:message_count].should eq 4
    end
  end

  it "supports publisher confirms" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm("m1").should be_true
    end
  end

  it "supports mandatory publish flag" do
    with_channel do |ch|
      pmsg = "m1"
      ch1 = Channel(Tuple(UInt16, String)).new
      ch.on_return do |msg|
        ch1.send({msg.reply_code, msg.reply_text})
      end
      ch.basic_publish(pmsg, "amq.direct", "none", mandatory: true)
      reply_code, reply_text = ch1.receive
      reply_code.should eq 312
      reply_text.should eq "No Route"
    end
  end

  it "expires messages" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "expired", props: AMQP::Client::Properties.new(expiration: "0")
      msg = q.get(no_ack: true)
      msg.should be_nil
    end
  end

  it "expires messages with message TTL on queue declaration" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-message-ttl"] = 1
      args["x-dead-letter-exchange"] = ""
      args["x-dead-letter-routing-key"] = "dlq"
      q = ch.queue("", args: args)
      dlq = ch.queue("dlq")
      q.publish_confirm "queue dlx"
      msg = wait_for { dlq.get(no_ack: true) }
      if msg
        msg.body_io.to_s.should eq("queue dlx")
      else
        msg.should_not be_nil
      end
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
  end

  it "dead-letter expired messages" do
    with_channel do |ch|
      dlq = ch.queue
      ch.queue("exp")

      hdrs = AMQP::Client::Arguments.new
      hdrs["x-dead-letter-exchange"] = ""
      hdrs["x-dead-letter-routing-key"] = dlq.name
      x = ch.exchange("", "direct", passive: true)
      x.publish_confirm "dead letter", "exp", props: AMQP::Client::Properties.new(expiration: "0", headers: hdrs)

      msg = wait_for { dlq.get(no_ack: true) }
      if msg
        msg.body_io.to_s.should eq("dead letter")
      else
        msg.should_not be_nil
      end
    end
  end

  it "handle immediate flag" do
    with_channel do |ch|
      pmsg = "m1"
      reply_code = 0
      reply_msg = nil
      ch.on_return do |msg|
        reply_code = msg.reply_code
        reply_msg = msg.reply_text
      end
      ch.basic_publish(pmsg, "amq.topic", "rk", mandatory: false, immediate: true)
      wait_for { reply_code == 313 }
      reply_code.should eq 313
    end
  end

  it "can cancel consumers" do
    with_channel do |ch|
      q = ch.queue("", auto_delete: false, durable: true, exclusive: false)
      q.publish "m1"
      msgs = [] of AMQP::Client::Message
      tag = q.subscribe { |msg| msgs << msg }
      q.unsubscribe(tag)
      Fiber.yield
      ch.has_subscriber?(tag).should eq false
    end
  end

  it "supports header exchange all" do
    with_channel do |ch|
      q = ch.queue
      hdrs = AMQP::Client::Arguments.new
      hdrs["x-match"] = "all"
      hdrs["org"] = "84codes"
      hdrs["user"] = "test"
      x = ch.exchange("asdasdasd", "headers", passive: false, args: hdrs)
      q.bind(x.name, "")
      x.publish "m1", q.name, props: AMQP::Client::Properties.new(headers: hdrs)
      hdrs["user"] = "hest"
      x.publish "m2", q.name, props: AMQP::Client::Properties.new(headers: hdrs)
      msgs = [] of AMQP::Client::Message
      q.subscribe { |msg| msgs << msg }
      until msgs.size == 1
        Fiber.yield
      end
      msgs.size.should eq 1
    end
  end

  it "supports header exchange any" do
    with_channel do |ch|
      q = ch.queue
      hdrs = AMQP::Client::Arguments.new
      hdrs["x-match"] = "any"
      hdrs["org"] = "84codes"
      hdrs["user"] = "test"
      x = ch.exchange("hx1", "headers", passive: false, args: hdrs)
      q.bind(x.name, "")
      x.publish "m1", q.name, props: AMQP::Client::Properties.new(headers: hdrs)
      hdrs["user"] = "hest"
      x.publish "m2", q.name, props: AMQP::Client::Properties.new(headers: hdrs)
      msgs = [] of AMQP::Client::Message
      q.subscribe { |msg| msgs << msg }
      Fiber.yield
      msgs.size.should eq 2
    end
  end

  it "splits frames into max frame sizes" do
    with_channel(port: 5672, frame_max: 4096_u32) do |ch|
      msg_size = (2**17 + 1)
      pmsg1 = "m" * msg_size
      q = ch.queue
      q.purge
      q.publish_confirm pmsg1
      msg = q.get(no_ack: true)
      msg.not_nil!.body_io.size.should eq msg_size
    end
  end

  it "can receive and deliver large messages" do
    with_channel do |ch|
      pmsg1 = "a" * 8133
      q = ch.queue
      q.publish pmsg1
      msg = q.get
      msg.not_nil!.body_io.to_s.should eq pmsg1.to_s
    end
  end

  it "acking an invalid delivery tag should close the channel" do
    with_channel do |ch|
      cch = Channel(Tuple(UInt16, String)).new
      ch.on_close do |code, text|
        cch.send({code, text})
      end
      ch.basic_ack(999_u64)
      code = cch.receive.first
      code.should eq 406
    end
  end

  it "can bind exchanges to exchanges" do
    with_channel do |ch|
      x1 = ch.exchange("x1", "direct")
      x2 = ch.exchange("x2", "direct")
      x2.bind(x1.name, "e2e")
      q = ch.queue("e2e", auto_delete: true, durable: false, exclusive: false)
      q.bind(x2.name, "e2e")
      pmsg = "test message"
      x1.publish_confirm pmsg, "e2e"
      Fiber.yield
      msg = q.get(no_ack: true)
      msg.not_nil!.body_io.to_s.should eq("test message")
    end
  ensure
    s.vhosts["/"].delete_exchange("x1")
    s.vhosts["/"].delete_exchange("x2")
    s.vhosts["/"].delete_queue("e2e")
  end

  it "supports x-max-length drop-head" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_i64
      q = ch.queue("", args: args)
      q.publish_confirm "m1"
      q.publish_confirm "m2"
      msgs = [] of AMQP::Client::Message
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "supports x-max-length reject-publish" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_i64
      args["x-overflow"] = "reject-publish"
      q = ch.queue("", args: args)
      q.publish_confirm("m1").should be_true
      q.publish_confirm("m2").should be_true
      msgs = [] of AMQP::Client::Message
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "disallows creating queues starting with amq." do
    expect_raises(AMQP::Client::Channel::ClosedException, /REFUSED/) do
      with_channel do |ch|
        ch.queue("amq.test")
      end
    end
  end

  it "disallows deleting exchanges named amq.*" do
    expect_raises(AMQP::Client::Channel::ClosedException, /REFUSED/) do
      with_channel do |ch|
        ch.exchange("amq.topic", "topic", passive: true).delete
      end
    end
    with_channel do |ch|
      ch.exchange("amq.topic", "topic", passive: true).should_not be_nil
    end
  end

  it "disallows creating new exchanges named amq.*" do
    expect_raises(AMQP::Client::Channel::ClosedException, /REFUSED/) do
      with_channel do |ch|
        ch.exchange("amq.topic2", "topic")
      end
    end
  end

  it "only allow one consumer on when exlusive consumers flag is set" do
    with_channel do |ch|
      q = ch.queue("exlusive_consumer", auto_delete: true)
      q.subscribe(exclusive: true) { }

      expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
        with_channel do |ch2|
          q2 = ch2.queue("exlusive_consumer", passive: true)
          q2.subscribe { }
        end
      end
    end
    with_channel do |ch|
      q = ch.queue("exlusive_consumer", auto_delete: true)
      q.subscribe { }
    end
  ensure
    s.vhosts["/"].delete_queue("exlusive_consumer")
  end

  it "only allow one connection access an exlusive queues" do
    with_channel do |ch|
      ch.queue("exlusive_queue", durable: true, exclusive: true)
      expect_raises(AMQP::Client::Channel::ClosedException, /RESOURCE_LOCKED/) do
        with_channel do |ch2|
          ch2.queue("exlusive_queue", passive: true)
        end
      end
    end
  ensure
    s.vhosts["/"].delete_queue("exlusive_consumer")
  end

  it "it persists transient msgs between restarts" do
    with_channel do |ch|
      q = ch.queue("durable_queue", durable: true)
      1000.times do |i|
        delivery_mode = i % 2 == 0 ? 2_u8 : 0_u8
        props = AMQP::Client::Properties.new(delivery_mode: delivery_mode)
        q.publish(i.to_s, props: props)
      end
    end
    wait_for { s.vhosts["/"].queues["durable_queue"].message_count == 1000 }
    close_servers
    TestHelpers.setup
    wait_for { s.vhosts["/"].queues["durable_queue"].message_count == 1000 }
    with_channel do |ch|
      q = ch.queue("durable_queue", durable: true)
      deleted_msgs = q.delete
      deleted_msgs[:message_count].should eq(1000)
    end
  ensure
    s.vhosts["/"].delete_queue("durable_queue")
  end

  it "supports max-length" do
    definitions = {"max-length" => JSON::Any.new(1_i64)}
    s.vhosts["/"].add_policy("ml", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 10_i8)
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm("m1").should be_true
      q.publish_confirm("m2").should be_true
      msgs = [] of AMQP::Client::Message
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  ensure
    s.vhosts["/"].delete_policy("ml")
  end

  it "supports alternate-exchange" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-alternate-exchange"] = "ae"
      x1 = ch.exchange("x1", "topic", args: args)
      ae = ch.exchange("ae", "topic")
      q = ch.queue
      q.bind(ae.name, "*")
      x1.publish("m1", "rk")
      msg = q.get(no_ack: true)
      msg.not_nil!.body_io.to_s.should eq("m1")
    end
  ensure
    s.vhosts["/"].delete_exchange("x1")
    s.vhosts["/"].delete_exchange("ae")
  end

  it "supports expires" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
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
    with_channel do |ch|
      q1 = ch.queue
      q2 = ch.queue
      x1 = ch.exchange("x122", "topic")
      q1.bind(x1.name, "rk")
      q2.bind(x1.name, "rk")
      x1.publish("m1", "rk")
      sleep 0.05
      msg_q1 = q1.get(no_ack: true)
      msg_q2 = q2.get(no_ack: true)
      msg_q1.not_nil!.body_io.to_s.should eq("m1")
      msg_q2.not_nil!.body_io.to_s.should eq("m1")
    end
  ensure
    s.vhosts["/"].delete_exchange("x122")
  end

  it "supports auto ack consumers" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      msgs = [] of AMQP::Client::Message
      q.subscribe(no_ack: true) { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "sets correct message timestamp" do
    with_channel do |ch|
      q = ch.queue
      t = Time.utc_now.to_unix
      q.publish "m1"
      msg = nil
      q.subscribe(no_ack: true) { |m| msg = m }
      wait_for { msg }
      msg.not_nil!.properties.timestamp.not_nil!.to_unix.should be_close(t, 1)
    end
  end

  it "supports recover requeue" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      delivered = 0
      q.subscribe(no_ack: false) { |_m| delivered += 1 }
      sleep 0.05
      ch.basic_recover(requeue: true)
      sleep 0.05
      delivered.should eq 2
    end
  end

  it "supports recover redeliver" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      msg = nil
      q.subscribe(no_ack: false) { |m| msg = m }
      wait_for { msg }
      msg = nil
      ch.basic_recover(requeue: true)
      wait_for { msg }
      msg.not_nil!.redelivered.should be_true
    end
  end

  it "supports recover for basic get" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      q.publish "m2"
      ch.basic_get(q.name, no_ack: false)
      ch.basic_recover(requeue: true)
      msg = ch.basic_get(q.name, no_ack: false)
      msg.not_nil!.body_io.to_s.should eq("m1")
    end
  end

  pending "compacts queue index correctly" do
    with_channel do |ch|
      q = ch.queue("durable_queue_index", durable: true)
      (AvalancheMQ::DurableQueue::MAX_ACKS + 1).times do |i|
        msg = i.to_s
        q.publish(msg)
      end
      AvalancheMQ::DurableQueue::MAX_ACKS.times do |_i|
        q.get(no_ack: false).try &.ack
      end
    end
    wait_for { s.vhosts["/"].queues["durable_queue_index"].message_count == 1 }
    close_servers
    TestHelpers.setup
    wait_for { s.vhosts["/"].queues["durable_queue_index"].message_count == 1 }
    with_channel do |ch|
      q = ch.queue("durable_queue_index", durable: true)
      deleted_msgs = q.delete
      deleted_msgs.should eq(1)
    end
  ensure
    s.vhosts["/"].delete_queue("durable_queue_index")
  end
end
