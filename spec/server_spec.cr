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
      m1.try(&.reject)
      m1 = q.get(no_ack: false)
      m1.should eq(nil)
    end
  end

  it "can reject and requeue message" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      m1 = q.get(no_ack: false)
      m1.try(&.reject(requeue: true))
      m1 = q.get(no_ack: false)
      m1.not_nil!.body_io.to_s.should eq("m1")
      m1.not_nil!.redelivered.should be_true
    end
  end

  it "can reject while consuming" do
    done = ::Channel(Nil).new(1)
    data = Bytes.new(1000)
    spawn do
      with_channel do |ch|
        q = ch.queue("reject")
        200.times do
          q.publish data
        end
      end
    end
    with_channel do |ch|
      q = ch.queue("reject")
      q.subscribe(no_ack: false) do |msg|
        msg.reject(requeue: false)
        done.send nil
      end
      timeout = false
      200.times do
        select
        when done.receive
        when timeout 1.seconds
          timeout = true
        end
      end
      timeout.should be_false
    end
    wait_for { s.connections.empty? }
  ensure
    s.vhosts["/"].delete_queue("reject")
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
      code = Channel(UInt16).new
      ch.on_close do |c, _reply|
        code.send c
      end
      x = ch.exchange("test_ad_exchange", "topic", durable: false, auto_delete: true)
      q = ch.queue
      q.bind(x.name, "")
      q.unbind(x.name, "")
      x.publish("m1", q.name)
      code.receive.should eq 404
      sleep 0.01
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

  it "should run GC on purge" do
    vhost = s.vhosts["/"]
    data_dir = vhost.data_dir
    current = Dir.glob(File.join(data_dir, "msgs.*")).size

    with_channel do |ch|
      q = ch.queue "my_durable_queue", durable: true, exclusive: true
      4.times do
        q.publish_confirm "a" * 1024**2, props: AMQP::Client::Properties.new(delivery_mode: 2)
      end

      after_publish = Dir.glob(File.join(data_dir, "msgs.*")).size
      (after_publish > current).should be_true

      q.purge

      sleep 0 # yield to other fiber for GC

      wait_for { !vhost.dirty? }

      after_purge = Dir.glob(File.join(data_dir, "msgs.*")).size
      (after_purge < after_publish).should be_true
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
      reply_text.should eq "NO_ROUTE"
    end
  end

  it "can handle messages going to no queue" do
    with_channel do |ch|
      ch.basic_publish_confirm("m1", "amq.direct", "none").should eq true
      ch.basic_publish_confirm("m2", "amq.direct", "none").should eq true
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

  it "expires multiple messages" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "expired", props: AMQP::Client::Properties.new(expiration: "1")
      sleep 0.2
      q.publish_confirm "expired", props: AMQP::Client::Properties.new(expiration: "1")
      sleep 0.2
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
      q = ch.queue("ttl", args: args)
      dlq = ch.queue("dlq")
      q.publish_confirm "queue dlx"
      msg = wait_for { dlq.get(no_ack: true) }
      msg.not_nil!.body_io.to_s.should eq("queue dlx")
      s.vhosts["/"].queues["ttl"].empty?.should be_true
      q.publish_confirm "queue dlx"
      msg = wait_for { dlq.get(no_ack: true) }
      msg.not_nil!.body_io.to_s.should eq("queue dlx")
    end
  ensure
    s.vhosts["/"].delete_queue("dlq")
    s.vhosts["/"].delete_queue("ttl")
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
      msg.not_nil!.body_io.to_s.should eq("dead letter")
    end
  ensure
    s.vhosts["/"].delete_queue("exp")
  end

  it "dead-letter expired messages to non existing exchange" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1
      args["x-dead-letter-exchange"] = "not_found"
      q = ch.queue "", durable: false, exclusive: true, args: args

      q.publish_confirm "1"
      q.publish_confirm "2"
      q.publish_confirm "3"
      msg = q.get(no_ack: true)
      msg.should_not be_nil
      msg.not_nil!.body_io.to_s.should eq "3"
    end
  end

  it "can publish to queues with max-length 0 if there's consumers" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 0
      q = ch.queue "", durable: false, exclusive: true, args: args
      mch = Channel(AMQP::Client::DeliverMessage).new(2)
      q.subscribe(no_ack: true) do |msg|
        mch.send msg
      end
      q.publish "1"
      q.publish "2"
      2.times do
        select
        when msg = mch.receive?
          msg.should_not be_nil
        when timeout 2.seconds
          raise "timeout"
        end
      end
    end
  end

  it "should still drop_overflow even with slow consumers" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 2
      q = ch.queue "", durable: false, exclusive: true, args: args
      mch = Channel(AMQP::Client::DeliverMessage).new(10)
      ch.prefetch 1
      q.subscribe(no_ack: false) do |msg|
        mch.send msg
        sleep 0.2
        msg.ack
      end
      10.times do |i|
        q.publish_confirm i.to_s
      end
      if m = mch.receive?
        m.body_io.to_s.should eq "0"
      end
      if m = mch.receive?
        m.body_io.to_s.should eq "8"
      end
      if m = mch.receive?
        m.body_io.to_s.should eq "9"
      end
    end
  end

  it "msgs publish to queue w/o consumers with max-length 0 will be dropped" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 0
      q = ch.queue "", durable: false, exclusive: true, args: args
      q.publish_confirm "1"
      q.publish_confirm "2"
      q.get.should be_nil
      q.get.should be_nil
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
    {% if flag?(:freebsd) %} pending! {% end %}
    with_channel do |ch|
      q = ch.queue("", auto_delete: false, durable: true, exclusive: false)
      q.publish "m1"
      msgs = [] of AMQP::Client::DeliverMessage
      tag = q.subscribe { |msg| msgs << msg }
      q.unsubscribe(tag)
      sleep 0.01
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
      x.publish_confirm "m1", q.name, props: AMQP::Client::Properties.new(headers: hdrs)
      hdrs["user"] = "hest"
      x.publish_confirm "m2", q.name, props: AMQP::Client::Properties.new(headers: hdrs)
      q.get.should_not be_nil
      q.get.should be_nil
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
      msgs = Channel(AMQP::Client::DeliverMessage).new(2)
      q.subscribe { |msg| msgs.send msg }
      spawn { sleep 5; msgs.close }
      2.times { msgs.receive?.should_not be_nil }
    end
  end

  it "validates header exchange x-match" do
    with_channel do |ch|
      hdrs = AMQP::Client::Arguments.new
      hdrs["x-match"] = "fail"
      expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
        ch.exchange("hx1", "headers", passive: false, args: hdrs)
      end
    end
  end

  it "validates header exchange binding with x-match" do
    with_channel do |ch|
      hdrs = AMQP::Client::Arguments.new
      hdrs["x-match"] = 123
      q = ch.queue
      expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
        q.bind("amq.headers", "", args: hdrs)
      end
    end
  end

  it "should persist exchange-exchange binding" do
    hdrs = AMQP::Client::Arguments.new
    hdrs["x-match"] = "all"
    hdrs["org"] = "84codes"
    hdrs["user"] = "test"

    with_channel do |ch|
      topic_x = ch.exchange("topic_exchange", "topic", passive: false)
      headers_x = ch.exchange("headers_exchange", "headers", passive: false)
      topic_x.bind(exchange: headers_x.name, routing_key: "", args: hdrs)
    end

    close_servers
    TestHelpers.setup

    with_channel do |ch|
      q = ch.queue
      q.bind("topic_exchange", "#")

      ch.basic_publish_confirm "m1", "headers_exchange", "test", props: AMQP::Client::Properties.new(headers: hdrs)

      hdrs["user"] = "hest"
      ch.basic_publish_confirm "m2", "headers_exchange", "test", props: AMQP::Client::Properties.new(headers: hdrs)

      q.get.should_not be_nil
      q.get.should be_nil
    end
  end

  it "splits frames into max frame sizes" do
    with_channel(port: 5672, frame_max: 4096_u32) do |ch|
      2.times do
        msg_size = (2**17 + 1)
        pmsg1 = "m" * msg_size
        sha1 = Digest::SHA1.digest pmsg1
        q = ch.queue
        q.purge
        q.publish_confirm pmsg1
        msg = q.get(no_ack: true)
        msg.not_nil!.body_io.size.should eq msg_size
        Digest::SHA1.digest(msg.not_nil!.body_io).should eq sha1
      end
    end
  end

  it "can receive and deliver multiple large messages" do
    pmsg = "a" * 150_000
    with_channel do |ch|
      2.times do
        q = ch.queue
        q.publish pmsg
        msg = q.get
        msg.not_nil!.body_io.to_s.should eq pmsg
      end
    end
  end

  it "acking an invalid delivery tag should close the channel" do
    with_channel do |ch|
      cch = Channel(Tuple(UInt16, String)).new
      ch.on_close do |code, text|
        cch.send({code, text})
      end
      ch.basic_ack(999_u64)
      c, _ = cch.receive
      c.should eq 406
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
      msgs = [] of AMQP::Client::DeliverMessage
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size >= 1 }
      msgs.size.should eq 1
    end
  end

  it "supports x-max-length reject-publish" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_i64
      args["x-overflow"] = "reject-publish"
      q1 = ch.queue("test1", durable: true, args: args)
      q2 = ch.queue("test2", durable: true)
      x1 = ch.exchange("x345", "topic")
      q1.bind(x1.name, "rk")
      q2.bind(x1.name, "rk")
      x1.publish_confirm("m1", "rk").should be_true
      x1.publish_confirm("m2", "rk").should be_false
      q1.publish_confirm("m3").should be_false
      q2.publish_confirm("m4").should be_true
      msgs = [] of AMQP::Client::DeliverMessage
      q1.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  ensure
    s.vhosts["/"].delete_exchange("x345")
  end

  it "drop overflow when max-length is applied" do
    with_channel do |ch|
      q = ch.queue("mlq")
      q.publish_confirm("m1").should be_true
      q.publish_confirm("m2").should be_true
      definitions = {
        "max-length" => JSON::Any.new(1_i64),
      } of String => JSON::Any
      s.vhosts["/"]
        .add_policy("test", /^mlq$/, AvalancheMQ::Policy::Target::Queues, definitions, 10_i8)
      sleep 0.01
      s.vhosts["/"].queues["mlq"].message_count.should eq 1
    end
  ensure
    s.vhosts["/"].delete_queue("mlq")
    s.vhosts["/"].delete_policy("test")
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

  it "it does persists transient msgs between restarts" do
    with_channel do |ch|
      ch.confirm_select
      q = ch.queue("durable_queue", durable: true)
      1000.times do |i|
        delivery_mode = i % 2 == 0 ? 2_u8 : 0_u8
        props = AMQP::Client::Properties.new(delivery_mode: delivery_mode)
        q.publish(i.to_s, props: props)
      end
      ch.wait_for_confirm(1000)
    end
    close_servers
    TestHelpers.setup
    with_channel do |ch|
      q = ch.queue("durable_queue", durable: true)
      deleted_msgs = q.delete
      deleted_msgs[:message_count].should eq 1000
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
      msgs = [] of AMQP::Client::DeliverMessage
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
      args["alternate-exchange"] = "ae"
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

  it "supports x-alternate-exchange" do
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
      msgs = [] of AMQP::Client::DeliverMessage
      q.subscribe(no_ack: true) { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
  end

  it "sets correct message timestamp" do
    AvalancheMQ::Config.instance.set_timestamp = true
    with_channel do |ch|
      q = ch.queue
      t = Time.utc.to_unix
      q.publish "m1"
      msg = nil
      q.subscribe(no_ack: true) { |m| msg = m }
      wait_for { msg }
      msg.not_nil!.properties.timestamp.not_nil!.to_unix.should be_close(t, 1)
    end
    AvalancheMQ::Config.instance.set_timestamp = false
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

  it "recover(requeue=false) for basic get actually reueues" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      q.publish "m2"
      ch.basic_get(q.name, no_ack: false)
      ch.basic_recover(requeue: false)
      msg = ch.basic_get(q.name, no_ack: false)
      msg.not_nil!.body_io.to_s.should eq("m1")
    end
  end

  it "supports delivery-limit" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-delivery-limit"] = 2
      q = ch.queue("delivery_limit", args: args)
      q.publish "m1"
      msg = q.get(no_ack: false).not_nil!
      msg.properties.headers.not_nil!["x-delivery-count"].as(Int32).should eq 1
      msg.reject(requeue: true)
      msg = q.get(no_ack: false).not_nil!
      msg.properties.headers.not_nil!["x-delivery-count"].as(Int32).should eq 2
      msg.reject(requeue: true)
      Fiber.yield
      q.get(no_ack: false).should be_nil
      s.vhosts["/"].queues["delivery_limit"].empty?.should be_true
    end
  end

  it "supports sends nack if all queues reject" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 0_i64
      args["x-overflow"] = "reject-publish"
      q = ch.queue("", args: args)
      q.publish_confirm("m1").should be_false
    end
  end

  it "binding to empty queue name binds last queue" do
    with_channel do |ch|
      q = ch.queue
      ch.queue_bind("", "amq.direct", "foo")
      ch.basic_publish_confirm("bar", "amq.direct", "foo")
      msg = q.get
      msg.should_not be_nil
      msg.not_nil!.body_io.to_s.should eq "bar"
    end
  end

  it "refuses binding to amq.default" do
    with_channel do |ch|
      q = ch.queue
      expect_raises(AMQP::Client::Channel::ClosedException, "ACCESS_REFUSED") do
        q.bind("", "foo")
      end
    end
  end

  it "refuses unbinding from amq.default" do
    with_channel do |ch|
      q = ch.queue
      expect_raises(AMQP::Client::Channel::ClosedException, "ACCESS_REFUSED") do
        q.unbind("", q.name)
      end
    end
  end

  it "requeues unacked msg from basic_ack on disconnect" do
    with_channel do |ch|
      q = ch.queue("ba", durable: true)
      q.publish "m1"
      msg = ch.basic_get(q.name, no_ack: false)
      msg.should_not be_nil
    end
    with_channel do |ch|
      q = ch.queue("ba", durable: true)
      msg = ch.basic_get(q.name, no_ack: true)
      msg.should_not be_nil
    end
  end

  it "will close channel on unknown delivery tag" do
    with_channel do |ch|
      ch.basic_ack(1)
      sleep 0.1
      expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED - unknown delivery tag 1/) do
        ch.basic_ack(1)
      end
    end
  end

  it "will close channel on double ack" do
    with_channel do |ch|
      q = ch.queue("")
      q.publish "1"
      q.publish "2"
      q.publish "3"
      msg1 = ch.basic_get(q.name, no_ack: false)
      msg2 = ch.basic_get(q.name, no_ack: false)
      _msg3 = ch.basic_get(q.name, no_ack: false)
      msg2.not_nil!.ack
      msg2.not_nil!.ack # this will trigger the error
      sleep 0.1
      expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED - unknown delivery tag 2/) do
        msg1.not_nil!.ack
      end
    end
  end

  it "will close channel on double ack with multiple" do
    with_channel do |ch|
      q = ch.queue("")
      q.publish "1"
      q.publish "2"
      q.publish "3"
      _msg1 = ch.basic_get(q.name, no_ack: false)
      msg2 = ch.basic_get(q.name, no_ack: false)
      msg3 = ch.basic_get(q.name, no_ack: false)
      msg2.not_nil!.ack
      msg2.not_nil!.ack(multiple: true) # this should tigger a precondition fail
      sleep 0.1
      expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED - unknown delivery tag 2/) do
        msg3.not_nil!.ack
      end
    end
  end

  it "will requeue messages that can't be delivered" do
    {% if flag?(:freebsd) %} pending! {% end %}
    qname = ("requeue-failed-delivery")
    count = 0
    with_channel do |ch|
      ch.@connection.@io.as(TCPSocket).linger = 0
      q = ch.queue(qname)
      q.subscribe(no_ack: false) do |_msg|
        count += 1
      end
      q.publish "1"
      q.publish "2"
      Fiber.yield
      ch.@connection.@io.as(TCPSocket).close
      Fiber.yield
      count.should eq 0

      Fiber.yield
      s.vhosts["/"].queues[qname].@ready.size.should eq 2
      s.vhosts["/"].queues[qname].@unacked.size.should eq 0
    end
  ensure
    if n = qname
      s.vhosts["/"].delete_queue(n)
    end
  end
end
