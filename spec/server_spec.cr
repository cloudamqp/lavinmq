require "./spec_helper"

describe LavinMQ::Server do
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
    spawn do
      with_channel do |ch|
        ch.prefetch 10
        q = ch.queue("reject")
        q.subscribe(no_ack: false, block: true) do |msg|
          msg.reject(requeue: false)
          done.send nil
        end
      end
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
    with_channel do |ch|
      ch.queue_declare("reject", passive: true)[:message_count].should eq 0
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
  end

  it "can delete exchange" do
    with_channel do |ch|
      x = ch.exchange("test_delete_exchange", "topic", durable: true)
      x.delete
      expect_raises(AMQP::Client::Channel::ClosedException) do
        ch.exchange("test_delete_exchange", "topic", durable: true, passive: true)
      end
    end
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
      q, dlq = create_ttl_and_dl_queues(ch)
      ttl_msg = "queue dlx"
      q.publish_confirm ttl_msg
      msg = wait_for { dlq.get(no_ack: true) }
      msg.not_nil!.body_io.to_s.should eq(ttl_msg)
      Server.vhosts["/"].queues[q.name].empty?.should be_true
      q.publish_confirm ttl_msg
      msg = wait_for { dlq.get(no_ack: true) }
      msg.not_nil!.body_io.to_s.should eq(ttl_msg)
    end
  end

  it "expires messages with TTL on requeue" do
    with_channel do |ch|
      q, dlq = create_ttl_and_dl_queues(ch, queue_ttl: 500)
      r_msg = "requeue msg"
      q.publish_confirm r_msg
      msg = q.get(no_ack: false).not_nil!
      msg.reject(requeue: true)
      msg = wait_for { dlq.get(no_ack: true) }
      msg.not_nil!.body_io.to_s.should eq(r_msg)
      Server.vhosts["/"].queues[q.name].empty?.should be_true
    end
  end

  it "can publish and consume messages larger than 128kb" do
    with_channel do |ch|
      lmsg = "a" * 5_00_000
      q = ch.queue "lmsg_q"
      q.publish_confirm lmsg
      q.subscribe(no_ack: true) do |msg|
        msg.should_not be_nil
        msg.body_io.to_s.should eq(lmsg)
      end
    end
  end

  it "does not requeue messages on consumer close" do
    with_channel do |ch|
      q = ch.queue "msg_q"
      q.publish_confirm "no requeue"
      done = Channel(Nil).new
      tag = q.subscribe(no_ack: false) { |_| done.send nil }
      done.receive
      q.unsubscribe(tag)
      Server.vhosts["/"].queues["msg_q"].empty?.should be_true
    end
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

  it "dead-letter preserves headers" do
    with_channel do |ch|
      dlq = ch.queue
      ch.queue("exp")

      hdrs = AMQP::Client::Arguments.new
      hdrs["custom1"] = "v1"
      hdrs["custom2"] = "v2"
      hdrs["x-dead-letter-exchange"] = ""
      hdrs["x-dead-letter-routing-key"] = dlq.name
      x = ch.exchange("", "direct", passive: true)
      x.publish_confirm "dead letter", "exp", props: AMQP::Client::Properties.new(expiration: "0", headers: hdrs)
      msg = wait_for { dlq.get(no_ack: true) }
      headers = msg.properties.headers.should_not be_nil
      headers.has_key?("custom1").should be_true
      headers.has_key?("custom2").should be_true
      headers["custom1"].should eq "v1"
      headers["custom2"].should eq "v2"
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
      mch.close
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

    Server.restart

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
    with_channel(frame_max: 4096_u32) do |ch|
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

  it "acking tag 0 with multiple=true should not close the channel" do
    with_channel do |ch|
      ch.basic_ack(0, multiple: true)
      sleep 0.01
      ch.basic_ack(0, multiple: true)
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
  end

  it "drop overflow when max-length is applied" do
    with_channel do |ch|
      q = ch.queue("mlq")
      q.publish_confirm("m1").should be_true
      q.publish_confirm("m2").should be_true
      definitions = {
        "max-length" => JSON::Any.new(1_i64),
      } of String => JSON::Any
      Server.vhosts["/"].add_policy("test", "^mlq$", "queues", definitions, 10_i8)
      sleep 0.01
      Server.vhosts["/"].queues["mlq"].message_count.should eq 1
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
      ch.wait_for_confirms
    end
    Server.restart
    with_channel do |ch|
      q = ch.queue("durable_queue", durable: true)
      deleted_msgs = q.delete
      deleted_msgs[:message_count].should eq 1000
    end
  end

  it "it does remeber acked msgs between restarts" do
    with_channel do |ch|
      ch.confirm_select
      q = ch.queue("q")
      1000.times do |i|
        q.publish("a")
      end
      ch.wait_for_confirms
      q.message_count.should eq 1000
      q.subscribe(no_ack: false, tag: "c", block: true) do |msg|
        msg.ack
        q.unsubscribe("c") if msg.delivery_tag == 1000
      end
      q.message_count.should eq 0
    end
    Server.restart
    with_channel do |ch|
      q = ch.queue("q")
      q.message_count.should eq 0
    end
  end

  it "supports max-length" do
    definitions = {"max-length" => JSON::Any.new(1_i64)}
    Server.vhosts["/"].add_policy("ml", "^.*$", "queues", definitions, 10_i8)
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm("m1").should be_true
      q.publish_confirm("m2").should be_true
      msgs = [] of AMQP::Client::DeliverMessage
      q.subscribe { |msg| msgs << msg }
      wait_for { msgs.size == 1 }
      msgs.size.should eq 1
    end
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
  end

  it "supports expires" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-expires"] = 1
      ch.queue("test", args: args)
      sleep 5.milliseconds
      Fiber.yield
      Server.vhosts["/"].queues.has_key?("test").should be_false
    end
  end

  it "does not expire queue when consumer are still there" do
    with_channel do |ch|
      args = AMQP::Client::Arguments.new
      args["x-expires"] = 50
      q = ch.queue("test", args: args)
      q.subscribe(no_ack: true) { |_| }
      sleep 50.milliseconds
      Fiber.yield
      Server.vhosts["/"].queues.has_key?("test").should be_true
    end
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
    LavinMQ::Config.instance.set_timestamp = true
    with_channel do |ch|
      q = ch.queue
      t = Time.utc.to_unix
      q.publish "m1"
      msg = nil
      q.subscribe(no_ack: true) { |m| msg = m }
      wait_for { msg }
      msg.not_nil!.properties.timestamp.not_nil!.to_unix.should be_close(t, 1)
    end
    LavinMQ::Config.instance.set_timestamp = false
  end

  it "supports recover requeue" do
    with_channel do |ch|
      q = ch.queue("recover", exclusive: true)
      5.times { q.publish "" }
      delivered = 0
      tag = q.subscribe(no_ack: false) { |_m| delivered += 1 }
      sleep 0.05
      q.unsubscribe(tag)
      sleep 0.05
      ch.basic_recover(requeue: true)
      sleep 0.05
      q.delete[:message_count].should eq 5
    end
  end

  it "supports recover redeliver" do
    with_channel do |ch|
      q = ch.queue
      q.publish "m1"
      msgs = Channel(AMQP::Client::DeliverMessage).new
      q.subscribe(no_ack: false) { |m| msgs.send m }
      msgs.receive
      ch.basic_recover(requeue: true)
      msgs.receive.redelivered.should be_true
    end
  end

  it "basic_recover requeues messages for cancelled consumers" do
    with_channel do |ch|
      q = ch.queue("q")
      q.publish "m1"
      msgs = Channel(AMQP::Client::DeliverMessage).new
      tag = q.subscribe(no_ack: false) { |m| msgs.send m }
      msgs.receive.body_io.to_s.should eq "m1"
      q.unsubscribe(tag)
      ch.basic_recover(requeue: true)
      select
      when m = msgs.receive
        m.should be_nil
      when timeout 100.milliseconds
      end
      ch.queue_declare(q.name, passive: true)[:message_count].should eq 1
    end
  end

  it "supports recover for basic get" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "m1"
      q.publish_confirm "m2"
      q.get(no_ack: false).not_nil!.body_io.to_s.should eq "m1"
      ch.basic_recover(requeue: true)
      q.get(no_ack: false).not_nil!.body_io.to_s.should eq "m1"
    end
  end

  it "recover(requeue=false) for basic get actually requeues" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "m1"
      q.publish_confirm "m2"
      q.get(no_ack: false).not_nil!.body_io.to_s.should eq "m1"
      ch.basic_recover(requeue: false)
      q.get(no_ack: false).not_nil!.body_io.to_s.should eq "m1"
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
      Server.vhosts["/"].queues["delivery_limit"].empty?.should be_true
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
    qname = "requeue-failed-delivery"
    count = 0
    with_channel do |ch|
      ch.@connection.@io.as(TCPSocket).linger = 0
      q = ch.queue(qname)
      q.subscribe(no_ack: false) do |_msg|
        count += 1
      end
      q.publish "1"
      ch.@connection.@io.as(TCPSocket).close
      Fiber.yield
      count.should eq 0

      Fiber.yield
      Server.vhosts["/"].queues[qname].message_count.should eq 1
      Server.vhosts["/"].queues[qname].unacked_count.should eq 0
    end
  end

  it "will limit message size" do
    LavinMQ::Config.instance.max_message_size = 128
    with_channel do |ch|
      expect_raises(AMQP::Client::Channel::ClosedException, /message size/) do
        ch.basic_publish_confirm("a" * 129, "amq.direct", "none")
      end
    end
  ensure
    LavinMQ::Config.instance.max_message_size = 128 * 1024**2
  end

  it "should measure time it takes to collect metrics in stats_loop" do
    stats_interval = LavinMQ::Config.instance.stats_interval
    LavinMQ::Config.instance.stats_interval = 100
    server = LavinMQ::Server.new("/tmp/lavinmq-spec")
    should_eventually(be_true, 1.seconds) { server.stats_collection_duration_seconds_total > Time::Span.zero }
    server.stats_rates_collection_duration_seconds.should_not eq Time::Span.zero
    server.stats_system_collection_duration_seconds.should_not eq Time::Span.zero
  ensure
    LavinMQ::Config.instance.stats_interval = stats_interval if stats_interval
    server.try &.close
  end

  it "should requeue messages correctly on channel close" do
    qname = "requeue-on-close"
    with_channel do |ch|
      q = ch.queue(qname)

      10.times { q.publish_confirm "" }

      q.get(no_ack: true)
      q.get(no_ack: false)
      q.get(no_ack: false)

      count = 0
      q.subscribe(no_ack: false) do |_msg|
        count += 1
      end
      wait_for { count == 7 }
    end
    with_channel do |ch|
      q = ch.queue(qname)
      q.message_count.should eq 9
    end
  end

  it "supports single active consumer" do
    with_channel do |ch|
      ch.prefetch 1
      q = ch.queue("sac", args: AMQ::Protocol::Table.new({"x-single-active-consumer": true}))
      begin
        msgs = Channel(Tuple(String, AMQP::Client::DeliverMessage)).new
        q.subscribe(no_ack: false, tag: "1") do |msg|
          msgs.send({"1", msg})
        end
        q.subscribe(no_ack: false, tag: "2") do |msg|
          msgs.send({"2", msg})
        end
        4.times { q.publish "" }
        2.times do
          tag, msg = msgs.receive
          tag.should eq "1"
          msg.ack
        end
        tag, msg = msgs.receive
        tag.should eq "1"
        q.unsubscribe "1"
        msg.ack
        tag, msg = msgs.receive
        tag.should eq "2"
        msg.ack
      ensure
        q.delete
      end
    end
  end

  it "can publish large messages to multiple queues" do
    with_channel do |ch|
      q1 = ch.queue
      q2 = ch.queue
      q1.bind("amq.fanout", "")
      q2.bind("amq.fanout", "")
      2.times do |i|
        ch.basic_publish_confirm(i.to_s * 200_000, "amq.fanout", "")
      end
      2.times do |i|
        if msg = q1.get
          msg.body_io.to_s.should eq i.to_s * 200_000
        else
          msg.should_not be_nil
        end
        if msg = q2.get
          msg.body_io.to_s.should eq i.to_s * 200_000
        else
          msg.should_not be_nil
        end
      end
    end
  end

  it "can publish small messages to multiple queues" do
    with_channel do |ch|
      q1 = ch.queue
      q2 = ch.queue
      q1.bind("amq.fanout", "")
      q2.bind("amq.fanout", "")
      2.times do |i|
        ch.basic_publish_confirm(i.to_s * 200, "amq.fanout", "")
      end
      2.times do |i|
        if msg = q1.get
          msg.body_io.to_s.should eq i.to_s * 200
        else
          msg.should_not be_nil
        end
        if msg = q2.get
          msg.body_io.to_s.should eq i.to_s * 200
        else
          msg.should_not be_nil
        end
      end
    end
  end

  it "supports consumer timeouts" do
    with_channel do |ch|
      q = ch.queue("", exclusive: true, args: AMQP::Client::Arguments.new({"x-consumer-timeout": 100}))
      q.publish_confirm("a")
      expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION/) do
        q.subscribe(no_ack: false, tag: "c", block: true) { }
      end
    end
  end

  it "restarts fast even with large messages" do
    data = Bytes.new 128 * 1024**2
    with_channel do |ch|
      q = ch.queue("large-messages")
      10.times do |i|
        q.publish_confirm(data)
      end
    end
    restart_time = Benchmark.realtime do
      restart_memory = Benchmark.memory do
        Server.restart
      end
      restart_memory.should be < 1 * 1024**2
    end
    restart_time.should be < 3.seconds # Some CI environments are slow
    with_channel do |ch|
      ch.prefetch 1
      q = ch.queue("large-messages")
      done = Channel(Nil).new
      q.subscribe(no_ack: false) do |msg|
        msg.body_io.bytesize.should eq 128 * 1024**2
        msg.ack
        done.send nil
      end
      10.times { done.receive }
    end
  end
end
