require "./spec_helper"

describe "Exchange" do
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
