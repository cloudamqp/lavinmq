require "./spec_helper"

# Uses DelayedExchangeQueue as the test subject since it's the simplest
# internal queue to create. The internal? check in client.cr applies to
# all internal queues (DelayedExchangeQueue, RetryQueue, etc.).
describe "Internal Queue Access" do
  x_name = "delayed-topic-internal-spec"
  internal_q_name = "amq.delayed-#{x_name}"
  x_args = AMQP::Client::Arguments.new({"x-delayed-exchange" => true})

  it "should deny passive declare on internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[internal_q_name]?.should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
          ch.queue_declare(internal_q_name, passive: true)
        end
      end
    end
  end

  it "should deny delete on internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[internal_q_name]?.should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
          ch.queue_delete(internal_q_name)
        end
      end
    end
  end

  it "should deny consume on internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[internal_q_name]?.should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
          ch.basic_consume(internal_q_name) { }
        end
      end
    end
  end

  it "should deny basic_get on internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[internal_q_name]?.should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
          ch.basic_get(internal_q_name, no_ack: true)
        end
      end
    end
  end

  it "should deny bind on internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[internal_q_name]?.should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
          ch.queue_bind(internal_q_name, "amq.direct", "test")
        end
      end
    end
  end

  it "should deny unbind on internal queue" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.exchange(x_name, "topic", args: x_args)
        s.vhosts["/"].queues[internal_q_name]?.should_not be_nil
        expect_raises(AMQP::Client::Channel::ClosedException, /ACCESS_REFUSED/) do
          ch.queue_unbind(internal_q_name, "amq.direct", "test")
        end
      end
    end
  end
end
