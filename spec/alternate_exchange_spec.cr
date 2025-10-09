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
end
