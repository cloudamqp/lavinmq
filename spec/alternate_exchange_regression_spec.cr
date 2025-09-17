require "./spec_helper"

describe "Alternate Exchange Regression" do
  it "only routes to alternate-exchange when no queues are bound" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        args = AMQP::Client::Arguments.new
        args["alternate-exchange"] = "ae"
        x1 = ch.exchange("x1", "topic", args: args)
        ae = ch.exchange("ae", "topic")
        q = ch.queue
        q.bind(ae.name, "*")
        x1.publish("m1", "rk")
        msg = q.get(no_ack: true)
        msg.not_nil!.body_io.to_s.should eq("m1")
        x1.publish("m2", "rk2")
        msg = q.get(no_ack: true)
        msg.should be_nil
      end
    end
  end
end