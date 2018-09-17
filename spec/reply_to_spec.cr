require "./spec_helper"

describe AvalancheMQ::Server do
  describe "amq.direct.reply-to" do
    it "should allow amq.direct.reply-to to be declared" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("amq.direct.reply-to")
        q.name.should eq "amq.direct.reply-to"
      end
    end

    it "should allow amq.rabbitmq.reply-to to be declared" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("amq.rabbitmq.reply-to")
        q.name.should eq "amq.rabbitmq.reply-to"
      end
    end

    it "should be able to consume amq.direct.reply-to" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        consumer_tag = ch.queue("amq.direct.reply-to").subscribe("tag", no_ack: true) { }
        consumer_tag.should eq "tag"
      end
    end

    it "should be able to consume amq.rabbitmq.reply-to" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        consumer_tag = ch.queue("amq.rabbitmq.reply-to").subscribe("tag", no_ack: true) { }
        consumer_tag.should eq "tag"
      end
    end

    it "should require consumer to be in no-ack mode" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        expect_raises(AMQP::ChannelClosed, /PRECONDITION_FAILED/) do
          ch.queue("amq.direct.reply-to").subscribe(no_ack: false) { }
        end
      end
    end

    it "should set reply-to" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        ch.queue("amq.direct.reply-to").subscribe(no_ack: true) { }
        reply_to = nil
        ch.queue("test").subscribe do |msg|
          reply_to = msg.properties.reply_to
        end
        props = AMQP::Protocol::Properties.new(reply_to: "amq.direct.reply-to")
        msg = AMQP::Message.new("test", props)
        ch.exchange("", "direct").publish(msg, "test")
        wait_for { reply_to }
        reply_to.should match /^amq\.direct\.reply-to\..+$/
      end
    end

    it "should reject publish if no amq.direct.reply-to consumer" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        props = AMQP::Protocol::Properties.new(reply_to: "amq.direct.reply-to")
        msg = AMQP::Message.new("test", props)
        ch.queue("test")
        e = ch.exchange("", "direct")
        expect_raises(AMQP::ChannelClosed, /PRECONDITION_FAILED/) do
          e.publish(msg, "test")
          ch.confirm
        end
      end
    end

    it "should be ok to declare reply-to queue to check if consumer is connected" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        queue = AMQP::Queue.new(ch, "amq.direct.reply-to.random", false,
          false, false, AMQP::Protocol::Table.new)
        resp = queue.declare
        resp[1].should eq 0
      end
    end

    it "should return on mandatory publish to a reply routing key" do
      AMQP::Connection.start do |conn|
        ch = conn.channel
        pmsg = AMQP::Message.new("m1")
        ch1 = Channel(Tuple(UInt16, String)).new
        ch.on_return do |code, text|
          ch1.send({code, text})
        end
        ch.publish(pmsg, "amq.direct", "amq.direct.reply-to.random", mandatory: true)
        reply_code, reply_text = ch1.receive
        reply_code.should eq 312
        reply_text.should eq "No Route"
      end
    end
  end
end
