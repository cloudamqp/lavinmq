require "./spec_helper"

describe "Alternate Exchange" do
  it "only routes to alternate-exchange when no queues are bound" do
    with_amqp_server do |s|
      args = AMQP::Client::Arguments.new
      args["alternate-exchange"] = "unroutables"

      with_channel(s) do |ch|
        topic_ex = ch.exchange("topic-with-ae", "topic", args: args)
        unroutables_ex = ch.exchange("unroutables", "topic")
        unroutables_q = ch.queue("unroutables")
        unroutables_q.bind(unroutables_ex.name, "*")

        # When we publish to topic_ex without bindings, the message should go thought the alternate exchange
        topic_ex.publish("m1", "rk")
        msg = unroutables_q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m1")

        # When we publish to topic_ex with queue bindings and exchange bindings with alternate-exchange,
        # and that exchange does not have any queues bound to it,
        # the message should go through the alternate exchange and the queue
        # (same behavior as RMQ)
        secondary_ex = ch.exchange("secondary", "headers", args: args)
        ch.exchange_bind(topic_ex.name, secondary_ex.name, "#")
        target_q = ch.queue("q")
        target_q.bind(topic_ex.name, "*")
        topic_ex.publish("m3", "rk3")
        msg = target_q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m3")
        msg = unroutables_q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m3")
      end
    end
  end

  it "doesn't use the alternate exchange when the message reached an exchange visited before" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange("ae", "fanout")
        ae_q = ch.queue("ae-q")
        ae_q.bind("ae", "")
        args = AMQP::Client::Arguments.new
        args["alternate-exchange"] = "ae"

        # Diamond: x -> e1 -> y -> q and x -> e2 -> y, where e2 has an AE
        x = ch.exchange("x", "fanout")
        ch.exchange("e1", "fanout")
        ch.exchange("e2", "fanout", args: args)
        ch.exchange("y", "fanout")
        q = ch.queue("q")
        q.bind("y", "")
        ch.exchange_bind("x", "e1", "")
        ch.exchange_bind("x", "e2", "")
        ch.exchange_bind("e1", "y", "")
        ch.exchange_bind("e2", "y", "")
        x.publish("m1", "rk")
        q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m1"
        ae_q.get(no_ack: true).should be_nil

        # Cycle: c1 <-> c2, both with an AE, c1 -> q
        c1 = ch.exchange("c1", "fanout", args: args)
        ch.exchange("c2", "fanout", args: args)
        ch.exchange_bind("c1", "c2", "")
        ch.exchange_bind("c2", "c1", "")
        q.bind("c1", "")
        c1.publish("m2", "rk")
        q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m2"
        ae_q.get(no_ack: true).should be_nil
      end
    end
  end

  it "doesn't use the alternate exchange when an exchange binding matched but routed nowhere" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange("ae", "fanout")
        ae_q = ch.queue("ae-q")
        ae_q.bind("ae", "")
        args = AMQP::Client::Arguments.new
        args["alternate-exchange"] = "ae"
        x = ch.exchange("x", "direct", args: args)
        ch.exchange("empty", "fanout")
        ch.exchange_bind("x", "empty", "rk")

        x.publish("m1", "rk")
        ae_q.get(no_ack: true).should be_nil
        # A routing key without a matching binding still uses the AE
        x.publish("m2", "other")
        ae_q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m2"
      end
    end
  end

  it "decides on the alternate exchange from the routing key and CC keys together" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange("ae", "fanout")
        ae_q = ch.queue("ae-q")
        ae_q.bind("ae", "")
        args = AMQP::Client::Arguments.new
        args["alternate-exchange"] = "ae"
        x = ch.exchange("x", "direct", args: args)
        q = ch.queue("q")
        q.bind("x", "rk")

        # The routing key matches, a CC key doesn't
        x.publish("m1", "rk", props: AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"CC" => ["none"]})))
        q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m1"
        ae_q.get(no_ack: true).should be_nil

        # Only a CC key matches
        x.publish("m2", "none", props: AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"CC" => ["rk"]})))
        q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m2"
        ae_q.get(no_ack: true).should be_nil

        # Nothing matches
        x.publish("m3", "none", props: AMQ::Protocol::Properties.new(headers: AMQ::Protocol::Table.new({"CC" => ["none2"]})))
        q.get(no_ack: true).should be_nil
        ae_q.get(no_ack: true).not_nil!.body_io.to_s.should eq "m3"
        ae_q.get(no_ack: true).should be_nil
      end
    end
  end
end
