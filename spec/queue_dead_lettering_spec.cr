require "./spec_helper"
require "./../src/lavinmq/amqp/queue"
require "./../src/lavinmq/rough_time"

module DeadLetteringSpec
  QUEUE_NAME     = "q"
  DLQ_NAME       = "dlq"
  DEFAULT_Q_ARGS = {
    "x-dead-letter-exchange"    => "",
    "x-dead-letter-routing-key" => DLQ_NAME,
  }

  class_property! q : AMQP::Client::Queue
  class_property! dlq : AMQP::Client::Queue
  class_property! channel : AMQP::Client::Channel
  class_property! server : LavinMQ::Server

  def self.publish_n(n : Int, q, *, props = AMQP::Client::Properties.new)
    1.upto(n) do |i|
      channel.default_exchange.publish_confirm("msg#{i}", q.name)
    end
  end

  def self.get_n(n : Int, q, *, no_ack = false, file = __FILE__, line = __LINE__)
    get_n(n, q, no_ack: no_ack, file: file, line: line) { }
  end

  def self.get1(q, *, no_ack = true, file = __FILE__, line = __LINE__)
    get_n(1, q, no_ack: no_ack, file: file, line: line) { }.first
  end

  def self.get1(q, *, no_ack = false, file = __FILE__, line = __LINE__, &block)
    get_n(1, q, no_ack: no_ack, file: file, line: line) do |msg|
      yield msg
    end.first
  end

  def self.get_n(n : Int, q, *, no_ack = false, file = __FILE__, line = __LINE__, &)
    result = Array(AMQP::Client::GetMessage).new(n)
    n.times do
      msg = wait_for(file: file, line: line) { q.get(no_ack: no_ack) }.should_not be_nil
      yield msg
      result << msg
    end
    result
  end

  def self.declareQ(qargs = DEFAULT_Q_ARGS)
    DeadLetteringSpec.q = channel.queue(QUEUE_NAME, args: AMQP::Client::Arguments.new(qargs))
  end

  def self.declareDLQ
    DeadLetteringSpec.dlq = channel.queue(DLQ_NAME)
  end

  def self.with_dead_lettering_setup(*, qargs = DEFAULT_Q_ARGS, &)
    with_amqp_server do |s|
      with_channel(s) do |ch|
        DeadLetteringSpec.channel = ch
        q = declareQ(qargs)
        dlq = declareDLQ
        yield q, dlq, ch, s
      end
    end
  end

  describe "Queue Dead Lettering" do
    describe "Basic Dead-Lettering Mechanisms" do
      it "should dead letter on nack with requeue=false" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(3, q)
          msgs = get_n(3, q)

          # Nack in "wrong" order
          msgs.pop.nack(requeue: false)
          msgs.each &.nack(requeue: false)

          expected = ["msg3", "msg1", "msg2"]

          msgs = Channel(String).new(expected.size)
          dlq.subscribe(block: false) do |msg|
            msgs.send(msg.body_io.to_s)
          end

          expected.each do |expected_body|
            msgs.receive.should eq expected_body
          end
        end
      end

      it "should dead letter many when nack with requeue=false and multiple=true" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(3, q)

          msgs = get_n(3, q)
          msgs.last.nack(multiple: true, requeue: false)

          expected = ["msg1", "msg2", "msg3"]

          msgs = Channel(String).new(expected.size)
          dlq.subscribe(block: false) do |msg|
            msgs.send(msg.body_io.to_s)
          end

          expected.each do |expected_body|
            msgs.receive.should eq expected_body
          end
        end
      end

      it "should not dead letter on nack with requeue=true" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(3, q)

          msgs = get_n(3, q)
          msgs.last.nack(requeue: true)

          wait_for { q.message_count == 1 }
          dlq.message_count.should eq 0
        end
      end

      it "should not dead letter on nack with requeue=true and multiple=true" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(3, q)
          msgs = get_n(3, q)
          msgs.last.nack(multiple: true, requeue: true)

          wait_for { q.message_count == 3 }
          dlq.message_count.should eq 0
        end
      end

      it "should dead letter on reject with requeue=false" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(3, q)

          get1(q, &.reject(requeue: false))

          dlq_msg = get1(dlq)
          dlq_msg.body_io.to_s.should eq "msg1"
          q.message_count.should eq 2
          dlq.message_count.should eq 0
        end
      end

      it "should dead letter in chain reject->expire->expire" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          b = ch.queue("b", args: AMQP::Client::Arguments.new({
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "a2",
          }))
          a2 = ch.queue("a2", args: AMQP::Client::Arguments.new({
            "x-message-ttl"             => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "a3",
          }))
          a3 = ch.queue("a3", args: AMQP::Client::Arguments.new({
            "x-message-ttl"             => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "b",
          }))

          ch.default_exchange.publish_confirm("msg", b.name)

          get1(b, &.reject(requeue: false))
          # Reject a second time, the message should be dead lettered
          # through the chain again. No cycle should be detected.
          get1(b, &.reject(requeue: false))

          msg = get1(b, &.ack)
          msg.body_io.to_s.should eq "msg"
        end
      end

      it "should dead letter many rejects" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(100, q)
          done = Channel(Nil).new
          spawn do
            get_n(100, q, &.reject(requeue: false))
            done.send nil
          end

          select
          when done.receive
          when timeout(5.seconds)
            fail "timeout rejecting messages"
          end

          msgs = get_n(100, dlq)

          1.upto(100) do |i|
            msgs.shift.body_io.to_s.should eq "msg#{i}"
          end
          dlq.message_count.should eq 0
        end
      end

      it "should not dead letter on reject with requeue=true" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          1.upto(3) do |i|
            channel.default_exchange.publish_confirm("msg#{i}", q.name)
          end

          msg1 = q.get(no_ack: false).not_nil!
          msg1.reject(requeue: true)

          wait_for { q.message_count == 3 }
          dlq.message_count.should eq 0
        end
      end

      it "should dead letter on ttl" do
        qargs = {
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dlx",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          q = ch.queue("q", args: AMQP::Client::Arguments.new({
            "x-message-ttl"             => 1,
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "dlx",
          }))
          dlx = ch.queue("dlx")

          ch.default_exchange.publish_confirm("msg1", q.name)

          msg = wait_for { dlx.get }.should_not be_nil
          msg.body_io.to_s.should eq "msg1"
          q.message_count.should eq 0
        end
      end

      it "should dead letter on max-length with drop-head" do
        qargs = {
          "x-max-length"              => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dlx",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlx = ch.queue("dlx")

          ch.default_exchange.publish_confirm("msg1", q.name)
          ch.default_exchange.publish_confirm("msg2", q.name)
          ch.default_exchange.publish_confirm("msg3", q.name)

          wait_for { dlx.message_count == 2 }
          q.message_count.should eq 1

          dlx_msg1 = dlx.get.should_not be_nil
          dlx_msg2 = dlx.get.should_not be_nil

          dlx_msg1.body_io.to_s.should eq "msg1"
          dlx_msg2.body_io.to_s.should eq "msg2"

          q_msg = q.get.should_not be_nil
          q_msg.body_io.to_s.should eq "msg3"
        end
      end
    end

    describe "Complex Rejection Scenarios" do
      it "should dead letter single on nack" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          ch.default_exchange.publish_confirm("msg", q.name)

          get1(q, &.reject(requeue: true))

          get1(q, &.reject(requeue: false))

          dlq_msg = get1(dlq)
          dlq_msg.body_io.to_s.should eq "msg"
          dlq.message_count.should eq 0
        end
      end

      it "should not dead letter nack multiple with requeue=true, but with requeue=false" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          queue = s.vhosts["/"].queues["q"].should be_a LavinMQ::AMQP::Queue

          publish_n(3, q)

          msgs = get_n(3, q)
          # Nack two first
          msgs[1].nack(multiple: true, requeue: true)

          # Wait for them to be requeued (unacked down to 1 from 3)
          wait_for { queue.unacked_count == 1 }

          # Get the two requeued messages
          msgs2 = get_n(2, q)
          # Nack the last with multiple, which means we ack all three unacked
          msgs2.last.nack(multiple: true, requeue: false)

          wait_for { dlq.message_count == 3 }

          msgs = get_n(3, dlq).map(&.body_io.to_s)

          msgs.should eq ["msg3", "msg1", "msg2"]
        end
      end

      it "dead_letter_nack_requeue_nack_norequeue_basic_consume" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(3, q)
          consumed = [] of AMQP::Client::DeliverMessage
          done = Channel(Nil).new

          # Get three but only ack the two first, then get these two
          # again and ack the last one which means ack all three currently
          # unacked.
          tag = q.subscribe(no_ack: false) do |msg|
            consumed << msg
            if consumed.size == 3
              consumed[1].nack(multiple: true, requeue: true)
            elsif consumed.size == 5
              consumed[4].nack(multiple: true, requeue: false)
              done.send nil
            end
          end

          done.should be_receiving nil

          ch.basic_cancel(tag)

          messages = get_n(3, dlq).map(&.body_io.to_s)
          messages.should eq ["msg3", "msg1", "msg2"]
        end
      end

      pending "dead_letter_max_length_reject_publish_dlx" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            q = ch.queue("q", args: AMQP::Client::Arguments.new({
              "x-max-length"              => 1,
              "x-overflow"                => "reject-publish-dlx",
              "x-dead-letter-exchange"    => "",
              "x-dead-letter-routing-key" => "dlx",
            }))
            dlx = ch.queue("dlx")

            ch.default_exchange.publish_confirm("msg1", q.name)
            ch.default_exchange.publish_confirm("msg2", q.name)
            ch.default_exchange.publish_confirm("msg3", q.name)

            wait_for { dlx.message_count == 2 }
            q.message_count.should eq 1

            q_msg = q.get
            q_msg.not_nil!.body_io.to_s.should eq "msg1"

            dlx_msg2 = dlx.get
            dlx_msg3 = dlx.get

            dlx_msg2.not_nil!.body_io.to_s.should eq "msg2"
            dlx_msg3.not_nil!.body_io.to_s.should eq "msg3"
          end
        end
      end
    end

    describe "Routing and Exchange Behavior" do
      it "dead_letter_missing_exchange" do
        qargs = {
          "x-dead-letter-exchange" => "missing_dlx",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          publish_n(2, q)

          get1(q, &.reject(requeue: false))
          sleep 0.1.seconds

          ch.exchange_declare("missing_dlx", "fanout", passive: false)
          dlq.bind("missing_dlx", "")

          get1(q, &.reject(requeue: false))

          dlq_msg = get1(dlq)
          dlq_msg.body_io.to_s.should eq "msg2"
          dlq.message_count.should eq 0
        end
      end

      it "dead_letter_routing_key" do
        qargs = {
          "x-dead-letter-exchange" => "dlx_exchange",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlx_exchange = ch.exchange_declare("dlx_exchange", "direct", passive: false)

          publish_n(2, q)
          get1(q, &.nack(requeue: false))

          sleep 0.1.seconds

          dlq.bind("dlx_exchange", q.name)

          msg = get1(q, &.nack(requeue: false))

          dlq_msg = get1(dlq)
          dlq_msg.body_io.to_s.should eq "msg2"
        end
      end

      it "dead_letter_routing_key_header_CC" do
        qargs = {
          "x-dead-letter-exchange" => "dlx_exchange",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlx_exchange = ch.exchange_declare("dlx_exchange", "direct", passive: false)
          dlq.bind("dlx_exchange", "dlq")

          props1 = AMQ::Protocol::Properties.new
          ch.basic_publish_confirm("msg1", "", q.name, props: props1)

          props2 = AMQ::Protocol::Properties.new(
            headers: AMQ::Protocol::Table.new({"CC" => [dlq.name] of AMQ::Protocol::Field})
          )
          ch.basic_publish_confirm("msg2", "", q.name, props: props2)

          get1(q, &.nack(requeue: false))

          sleep 0.05.seconds
          get1(q, &.nack(requeue: false))

          dlq_msg = get1(dlq)
          dlq_msg.body_io.to_s.should eq "msg2"
        end
      end

      it "dead_letter_routing_key_header_BCC" do
        qargs = {
          "x-dead-letter-exchange" => "dlx_exchange",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlx_exchange = ch.exchange_declare("dlx_exchange", "direct", passive: false)
          dlq.bind("dlx_exchange", "dlq")

          props1 = AMQ::Protocol::Properties.new
          ch.basic_publish_confirm("msg1", "", q.name, props: props1)

          props2 = AMQ::Protocol::Properties.new(
            headers: AMQ::Protocol::Table.new({"BCC" => [dlq.name] of AMQ::Protocol::Field})
          )
          ch.basic_publish_confirm("msg2", "", q.name, props: props2)

          get1(q, &.nack(requeue: false))
          sleep 0.05.seconds

          get1(q, &.nack(requeue: false))

          dlq_msg = get1(dlq)
          dlq_msg.body_io.to_s.should eq "msg2"
        end
      end

      pending "dead_letter_extra_bcc" do
        with_amqp_server do |s|
          v = s.vhosts["/"]

          source_args = AMQ::Protocol::Table.new({
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "target",
          })
          target_args = AMQ::Protocol::Table.new({
            "x-extra-bcc" => "extra_bcc",
          })

          v.declare_queue("source", true, false, source_args)
          v.declare_queue("target", true, false, target_args)
          v.declare_queue("extra_bcc", true, false, AMQ::Protocol::Table.new)

          props = AMQ::Protocol::Properties.new(expiration: "0")
          msg = LavinMQ::Message.new(RoughTime.unix_ms, "", "source", props, 3, IO::Memory.new("msg"))
          v.publish msg

          should_eventually(eq(1)) { v.queues["target"].message_count }
          should_eventually(eq(1)) { v.queues["extra_bcc"].message_count }
        end
      end
    end

    describe "Cycle Detection" do
      it "dead_letter_routing_key_cycle_max_length" do
        qargs = {
          "x-max-length"              => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => QUEUE_NAME,
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          ch.default_exchange.publish_confirm("msg1", q.name)
          ch.default_exchange.publish_confirm("msg2", q.name)

          sleep 0.1.seconds

          q.message_count.should eq 1
          msg = get1(q)
          msg.not_nil!.body_io.to_s.should eq "msg2"
        end
      end

      it "dead_letter_routing_key_cycle_ttl" do
        qargs = {
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => QUEUE_NAME,
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          publish_n(2, q)

          sleep 0.1.seconds

          q.message_count.should eq 0
        end
      end

      it "dead_letter_routing_key_cycle_with_reject" do
        qargs = {
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => QUEUE_NAME,
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          ch.default_exchange.publish_confirm("msg1", q.name)

          get1(q, &.nack(requeue: false))
          get1(q, &.nack(requeue: false))

          msg = get1(q)
          msg.body_io.to_s.should eq "msg1"
        end
      end
    end

    describe "Header Validation" do
      it "should add x-death for reject" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          ch.default_exchange.publish_confirm("msg1", q.name)

          get1(q, &.nack(requeue: false))

          dlq_msg = get1(dlq)

          headers = dlq_msg.properties.headers.should_not be_nil
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          x_death.size.should eq 1

          death_entry = x_death[0].as(AMQ::Protocol::Table)
          death_entry["queue"].should eq q.name
          death_entry["reason"].should eq "rejected"
          death_entry["count"].should eq 1
          death_entry["exchange"].should eq ""
          routing_keys = death_entry["routing-keys"].as(Array(AMQ::Protocol::Field))
          routing_keys.should eq [q.name]
        end
      end

      it "should add x-death for ttl" do
        qargs = {
          "x-message-ttl"             => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => DLQ_NAME,
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          ch.default_exchange.publish_confirm("msg1", q.name)

          dlq_msg = get1(dlq)

          headers = dlq_msg.properties.headers.should_not be_nil
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          death_entry = x_death[0].as(AMQ::Protocol::Table)
          death_entry["reason"].should eq "expired"
          death_entry.has_key?("original-expiration").should be_false
        end
      end

      it "should add x-death for message expiration" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          props = AMQ::Protocol::Properties.new(expiration: "1")
          ch.default_exchange.publish_confirm("msg1", q.name, props: props)
          ch.default_exchange.publish_confirm("msg2", q.name)

          dlq_msg = get1(dlq)

          headers = dlq_msg.not_nil!.properties.headers.should_not be_nil
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          death_entry = x_death[0].as(AMQ::Protocol::Table)
          death_entry["reason"].should eq "expired"
          death_entry["original-expiration"].should eq "1"
        end
      end

      it "dead_letter_headers_reason_maxlen" do
        qargs = {
          "x-max-length"              => 1,
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => DLQ_NAME,
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          publish_n(2, q)
          dlq_msg = get1(dlq)

          headers = dlq_msg.properties.headers.should_not be_nil
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          death_entry = x_death[0].as(AMQ::Protocol::Table)
          death_entry["reason"].should eq "maxlen"
        end
      end

      it "dead_letter_headers_cycle" do
        qargs = {
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "q",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          publish_n(1, q)
          get1(q, &.nack(requeue: false))

          msg = get1(q, no_ack: false)
          headers2 = msg.properties.headers.should_not be_nil
          x_death2 = headers2["x-death"].as(Array(AMQ::Protocol::Field))
          death_entry2 = x_death2.find { |d| d.as(AMQ::Protocol::Table)["queue"] == q.name }
          death_entry2.as(AMQ::Protocol::Table)["count"].should eq 1
          msg.nack(requeue: false)

          msg = get1(q)
          headers3 = msg.properties.headers.should_not be_nil
          x_death3 = headers3["x-death"].as(Array(AMQ::Protocol::Field))
          death_entry3 = x_death3.find { |d| d.as(AMQ::Protocol::Table)["queue"] == q.name }
          death_entry3.as(AMQ::Protocol::Table)["count"].should eq 2
        end
      end

      it "dead_letter_headers_should_be_appended_for_each_event" do
        qargs = {
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dlq1",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlq1 = ch.queue("dlq1", args: AMQP::Client::Arguments.new({
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => "dlq2",
          }))
          dlq2 = ch.queue("dlq2")

          publish_n(1, q)
          get1(q, &.nack(requeue: false))

          get1(dlq1, &.nack(requeue: false))

          msg_dlq2 = get1(dlq2)
          headers = msg_dlq2.properties.headers.should_not be_nil
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          x_death.size.should eq 2

          x_death[0].as(AMQ::Protocol::Table)["queue"].should eq dlq1.name
          x_death[1].as(AMQ::Protocol::Table)["queue"].should eq q.name
        end
      end

      it "dead_letter_headers_should_not_be_appended_for_republish" do
        with_dead_lettering_setup do |q, dlq, ch, s|
          publish_n(1, q)
          get1(q, &.nack(requeue: false))

          dlq_msg = get1(dlq)
          headers = dlq_msg.properties.headers.not_nil!
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          x_death.size.should eq 1
          x_death[0].as(AMQ::Protocol::Table)["reason"].should eq "rejected"

          q.delete
          q = declareQ({
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => dlq.name,
            "x-message-ttl"             => 1,
          })

          props = AMQ::Protocol::Properties.new(headers: headers)
          publish_n(1, q, props: props)

          dlq_msg2 = get1(dlq)
          headers = dlq_msg2.properties.headers.not_nil!
          x_death = headers["x-death"].as(Array(AMQ::Protocol::Field))
          x_death[0].as(AMQ::Protocol::Table)["reason"].should eq "expired"
        end
      end

      pending "dead_letter_headers_CC" do
        qargs = {
          "x-dead-letter-exchange" => "dlx_exchange",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlx_exchange = ch.exchange_declare("dlx_exchange", "direct", passive: false)
          q.bind("dlx_exchange", "q")
          dlq.bind("dlx_exchange", "dlq")

          props = AMQ::Protocol::Properties.new(
            headers: AMQ::Protocol::Table.new({"CC" => [dlq.name] of AMQ::Protocol::Field})
          )
          ch.basic_publish_confirm("msg1", "dlx_exchange", q.name, props: props)

          dlx.get

          msg1 = q.get(no_ack: false).not_nil!
          msg1.nack(requeue: false)

          dlq_msg = wait_for { dlq.get }
          headers = dlq_msg.not_nil!.properties.headers.should_not be_nil
          headers.has_key?("CC").should be_true
          headers["x-death"].should_not be_nil
        end
      end

      pending "dead_letter_headers_CC_with_routing_key" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            dlx_exchange = ch.exchange_declare("dlx_exchange", "direct", passive: false)
            q = ch.queue("q", args: AMQP::Client::Arguments.new({
              "x-dead-letter-exchange"    => "dlx_exchange",
              "x-dead-letter-routing-key" => "dlx",
            }))
            dlx = ch.queue("dlx")
            q.bind("dlx_exchange", "q")
            dlx.bind("dlx_exchange", "dlx")

            props = AMQ::Protocol::Properties.new(
              headers: AMQ::Protocol::Table.new({"CC" => ["dlx"] of AMQ::Protocol::Field})
            )
            ch.basic_publish_confirm("msg1", "dlx_exchange", "q", props: props)

            dlx.get

            msg1 = q.get(no_ack: false).not_nil!
            msg1.nack(requeue: false)

            dlx_msg = wait_for { dlx.get }
            headers = dlx_msg.not_nil!.properties.headers.should_not be_nil
            headers.has_key?("CC").should be_true
            headers["x-death"].should_not be_nil
          end
        end
      end

      pending "dead_letter_headers_BCC" do
        with_amqp_server do |s|
          with_channel(s) do |ch|
            dlx_exchange = ch.exchange_declare("dlx_exchange", "direct", passive: false)
            q = ch.queue("q", args: AMQP::Client::Arguments.new({
              "x-dead-letter-exchange" => "dlx_exchange",
            }))
            dlx = ch.queue("dlx")
            q.bind("dlx_exchange", "q")
            dlx.bind("dlx_exchange", "dlx")

            props = AMQ::Protocol::Properties.new(
              headers: AMQ::Protocol::Table.new({
                "CC"  => ["cc_queue"] of AMQ::Protocol::Field,
                "BCC" => ["dlx"] of AMQ::Protocol::Field,
              })
            )
            ch.basic_publish_confirm("msg1", "dlx_exchange", "q", props: props)

            dlx.get

            msg1 = q.get(no_ack: false).not_nil!
            msg1.nack(requeue: false)

            dlx_msg = wait_for { dlx.get }
            headers = dlx_msg.not_nil!.properties.headers.should_not be_nil
            headers["CC"].should_not be_nil
            headers.has_key?("BCC").should be_false
          end
        end
      end

      it "dead_letter_headers_first_death" do
        qargs = {
          "x-dead-letter-exchange"    => "",
          "x-dead-letter-routing-key" => "dlq2",
        }
        with_dead_lettering_setup(qargs: qargs) do |q, dlq, ch, s|
          dlq2 = ch.queue("dlq2", args: AMQP::Client::Arguments.new({
            "x-dead-letter-exchange"    => "",
            "x-dead-letter-routing-key" => q.name,
          }))

          publish_n(1, q)
          get1(q, &.nack(requeue: false))

          msg_dlq = get1(dlq2, &.nack(requeue: false))

          msg2 = get1(q)
          headers = msg2.not_nil!.properties.headers.should_not be_nil
          headers["x-first-death-reason"].should eq "rejected"
          headers["x-first-death-queue"].should eq q.name
          headers["x-first-death-exchange"].should eq ""
        end
      end

      pending "dead_letter_headers_first_death_route (REQUIRES x-match = all-with-x)" do
        with_amqp_server do |s|
          v = s.vhosts["/"]

          headers_exchange = v.exchanges["amq.headers"]

          q1_args = AMQ::Protocol::Table.new({
            "x-message-ttl"          => 1,
            "x-dead-letter-exchange" => "amq.headers",
          })
          q2_args = AMQ::Protocol::Table.new({
            "x-message-ttl"          => 1,
            "x-dead-letter-exchange" => "amq.headers",
          })

          v.declare_queue("q1", true, false, q1_args)
          v.declare_queue("q2", true, false, q2_args)
          v.declare_queue("dlx_expired", true, false, AMQ::Protocol::Table.new)
          v.declare_queue("dlx_rejected", true, false, AMQ::Protocol::Table.new)

          v.bind_queue("dlx_expired", "amq.headers", "", AMQ::Protocol::Table.new({
            "x-match"              => "all-with-x",
            "x-first-death-reason" => "expired",
            "x-first-death-queue"  => "q1",
          }))
          v.bind_queue("dlx_rejected", "amq.headers", "", AMQ::Protocol::Table.new({
            "x-match"              => "all-with-x",
            "x-first-death-reason" => "rejected",
            "x-first-death-queue"  => "q2",
          }))

          msg1 = LavinMQ::Message.new(RoughTime.unix_ms, "", "q1", AMQ::Protocol::Properties.new, 4, IO::Memory.new("msg1"))
          v.publish msg1

          msg2 = LavinMQ::Message.new(RoughTime.unix_ms, "", "q2", AMQ::Protocol::Properties.new, 4, IO::Memory.new("msg2"))
          v.publish msg2

          should_eventually(eq(1)) { v.queues["dlx_expired"].message_count }

          v.queues["q2"].basic_get(no_ack: false) do |env|
            v.queues["q2"].reject(env.segment_position, requeue: false)
          end

          should_eventually(eq(1)) { v.queues["dlx_rejected"].message_count }
        end
      end
    end
  end
end
