require "./spec_helper"

describe AvalancheMQ::Server do
  describe "amq.direct.reply-to" do
    it "should allow amq.direct.reply-to to be declared" do
      s = amqp_server
      listen(s, 5672)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("amq.direct.reply-to")
        q.name.should eq "amq.direct.reply-to"
      end
    ensure
      close(s)
    end

    it "should allow amq.rabbitmq.reply-to to be declared" do
      s = amqp_server
      listen(s, 5672)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        q = ch.queue("amq.rabbitmq.reply-to")
        q.name.should eq "amq.rabbitmq.reply-to"
      end
    ensure
      close(s)
    end

    it "should be able to consume amq.direct.reply-to" do
      s = amqp_server
      listen(s, 5672)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        consumer_tag = ch.queue("amq.direct.reply-to").subscribe("tag", no_ack: true) {}
        consumer_tag.should eq "tag"
      end
    ensure
      close(s)
    end

    it "should be able to consume amq.rabbitmq.reply-to" do
      s = amqp_server
      listen(s, 5672)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        consumer_tag = ch.queue("amq.rabbitmq.reply-to").subscribe("tag", no_ack: true) {}
        consumer_tag.should eq "tag"
      end
    ensure
      close(s)
    end

    it "should require consumer to be in no-ack mode" do
      s = amqp_server
      listen(s, 5672)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        expect_raises(AMQP::ChannelClosed, /PRECONDITION_FAILED/) do
          consumer_tag = ch.queue("amq.direct.reply-to").subscribe(no_ack: false) {}
        end
      end
    ensure
      close(s)
    end

    it "should set reply-to" do
      s = amqp_server
      listen(s, 5672)
      AMQP::Connection.start do |conn|
        ch = conn.channel
        ch.queue("amq.direct.reply-to").subscribe(no_ack: true) {}
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
    ensure
      close(s)
    end

    it "should reject publish if no amq.direct.reply-to consumer" do
      s = amqp_server
      listen(s, 5672)
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
    ensure
      close(s)
    end
  end
end
