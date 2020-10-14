require "./spec_helper"

describe AvalancheMQ::Exchange do
  describe "Exchange => Exchange binding" do
    it "should allow multiple e2e bindings" do
      with_channel do |ch|
        x1 = ch.exchange("e1", "topic")
        x2 = ch.exchange("e2", "topic")
        x2.bind(x1.name, "#")

        q1 = ch.queue
        q1.bind(x2.name, "#")

        x1.publish "test message", "some-rk"

        q1.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("test message")
        q1.get(no_ack: true).should be_nil

        x3 = ch.exchange("e3", "topic")
        x3.bind(x1.name, "#")

        q2 = ch.queue
        q2.bind(x3.name, "#")

        x1.publish "test message", "some-rk"

        q1.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("test message")
        q1.get(no_ack: true).should be_nil

        q2.get(no_ack: true).try { |msg| msg.body_io.to_s }.should eq("test message")
        q2.get(no_ack: true).should be_nil
      end
    ensure
      s.vhosts["/"].exchanges.each_key do |e|
        s.vhosts["/"].delete_exchange(e)
      end
    end



  end

  describe "metrics" do
    x_name = "metrics"
    it "should count unroutable metrics" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new()
        x = ch.exchange(x_name, "topic", args: x_args)
        x.publish_confirm "test message 1", "none"
        s.vhosts["/"].exchanges[x_name].unroutable_count.should eq 1
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

    it "should count unroutable metrics" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new()
        x = ch.exchange(x_name, "topic", args: x_args)
        q = ch.queue
        q.bind(x.name, q.name)
        x.publish_confirm "test message 1", "none"
        x.publish_confirm "test message 2", q.name
        s.vhosts["/"].exchanges[x_name].unroutable_count.should eq 1
      end
    ensure
      s.vhosts["/"].delete_exchange(x_name)
    end

  end
end
