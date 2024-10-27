require "./spec_helper"

describe LavinMQ::Exchange do
  describe "Exchange => Exchange binding" do
    it "should allow multiple e2e bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")

          q1 = ch.queue
          q1.bind(x2.name, "#")

          x1.publish "test message", "some-rk"

          q1.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
          q1.get(no_ack: true).should be_nil

          x3 = ch.exchange("e3", "topic", auto_delete: true)
          x3.bind(x1.name, "#")

          q2 = ch.queue
          q2.bind(x3.name, "#")

          x1.publish "test message", "some-rk"

          q1.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
          q1.get(no_ack: true).should be_nil

          q2.get(no_ack: true).try(&.body_io.to_s).should eq("test message")
          q2.get(no_ack: true).should be_nil
        end
      end
    end
  end

  describe "metrics" do
    x_name = "metrics"
    it "should count unroutable metrics" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x_args = AMQP::Client::Arguments.new
          x = ch.exchange(x_name, "topic", args: x_args)
          x.publish_confirm "test message 1", "none"
          s.vhosts["/"].exchanges[x_name].unroutable_count.should eq 1
        end
      end
    end

    it "should count unroutable metrics" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x_args = AMQP::Client::Arguments.new
          x = ch.exchange(x_name, "topic", args: x_args)
          q = ch.queue
          q.bind(x.name, q.name)
          x.publish_confirm "test message 1", "none"
          x.publish_confirm "test message 2", q.name
          s.vhosts["/"].exchanges[x_name].unroutable_count.should eq 1
        end
      end
    end
  end
  describe "auto delete exchange" do
    it "should delete the exhchange when the last binding is removed" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange("ad", "topic", auto_delete: true)
          q = ch.queue
          q.bind(x.name, q.name)
          q2 = ch.queue
          q2.bind(x.name, q2.name)
          q.unbind(x.name, q.name)
          q2.unbind(x.name, q2.name)
          expect_raises(AMQP::Client::Channel::ClosedException) do
            ch.exchange("ad", "topic", passive: true)
          end
        end
      end
    end
  end

  describe "delayed message exchange declaration" do
    dmx_args = AMQP::Client::Arguments.new({"x-delayed-type" => "topic", "test" => "hello"})
    illegal_dmx_args = AMQP::Client::Arguments.new({"test" => "hello"})

    it "should declare delayed message exchange" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("test", "x-delayed-message", args: dmx_args)
        end
      end
    end

    it "should raise and not declare delayed message exchange if missing x-delayed-type argument" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
            ch.exchange("test2", "x-delayed-message", args: illegal_dmx_args)
          end
          s.vhosts["/"].exchanges["test2"]?.should be_nil
        end
      end
    end

    it "should redeclare same delayed message exchange" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("test3", "x-delayed-message", args: dmx_args)
          ch.exchange("test3", "x-delayed-message", args: dmx_args)
        end
      end
    end

    it "should raise exception when redeclaring exchange with mismatched arguments" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("test4", "x-delayed-message", args: dmx_args)
          expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
            ch.exchange("test4", "x-delayed-message", args: illegal_dmx_args)
          end
        end
      end
    end
  end

  describe "in_use?" do
    it "should not be in use when just created" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          ch.exchange("e1", "topic", auto_delete: true)
          ch.exchange("e2", "topic", auto_delete: true)
          s.vhosts["/"].exchanges["e1"].in_use?.should be_false
          s.vhosts["/"].exchanges["e2"].in_use?.should be_false
        end
      end
    end
    it "should be in use when it has bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchanges["e2"].in_use?.should be_true
        end
      end
    end
    it "should be in use when other exchange has binding to it" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic", auto_delete: true)
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchanges["e1"].in_use?.should be_true
        end
      end
    end

    it "should be in use when it has bindings" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x1 = ch.exchange("e1", "topic")
          x2 = ch.exchange("e2", "topic", auto_delete: true)
          x2.bind(x1.name, "#")
          s.vhosts["/"].exchanges["e1"].in_use?.should be_true
          x2.unbind(x1.name, "#")
          s.vhosts["/"].exchanges["e1"].in_use?.should be_false
        end
      end
    end
  end
end
