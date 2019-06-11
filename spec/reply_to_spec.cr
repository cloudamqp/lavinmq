require "./spec_helper"

describe AvalancheMQ::Server do
  describe "amq.direct.reply-to" do
    it "should allow amq.direct.reply-to to be declared" do
      with_channel do |ch|
        q = ch.queue("amq.direct.reply-to")
        q.name.should eq "amq.direct.reply-to"
      end
    ensure
      s.vhosts["/"].delete_queue("amq.direct.reply-to")
    end

    it "should allow amq.rabbitmq.reply-to to be declared" do
      with_channel do |ch|
        q = ch.queue("amq.rabbitmq.reply-to")
        q.name.should eq "amq.rabbitmq.reply-to"
      end
    ensure
      s.vhosts["/"].delete_queue("amq.rabbitmq.reply-to")
    end

    it "should be able to consume amq.direct.reply-to" do
      with_channel do |ch|
        consumer_tag = ch.queue("amq.direct.reply-to").subscribe("tag", no_ack: true) { }
        consumer_tag.should eq "tag"
      end
    ensure
      s.vhosts["/"].delete_queue("amq.direct.reply-to")
    end

    it "should be able to consume amq.rabbitmq.reply-to" do
      with_channel do |ch|
        consumer_tag = ch.queue("amq.rabbitmq.reply-to").subscribe("tag", no_ack: true) { }
        consumer_tag.should eq "tag"
      end
    ensure
      s.vhosts["/"].delete_queue("amq.rabbitmq.reply-to")
    end

    it "should require consumer to be in no-ack mode" do
      expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
        with_channel do |ch|
          ch.queue("amq.direct.reply-to").subscribe(no_ack: false) { }
        end
      end
    ensure
      s.vhosts["/"].delete_queue("amq.direct.reply-to")
    end

    it "should set reply-to" do
      with_channel do |ch|
        ch.queue("amq.direct.reply-to").subscribe(no_ack: true) { }
        q = ch.queue("test")
        props = AMQ::Protocol::Properties.new(reply_to: "amq.direct.reply-to")
        q.publish_confirm("test", props: props)
        reply_to = q.get.not_nil!.properties.reply_to
        reply_to.should match /^amq\.direct\.reply-to\..+$/
      end
    ensure
      s.vhosts["/"].delete_queue("amq.direct.reply-to")
      s.vhosts["/"].delete_queue("test")
    end

    it "should reject publish if no amq.direct.reply-to consumer" do
      expect_raises(AMQP::Client::Channel::ClosedException, /PRECONDITION_FAILED/) do
        with_channel do |ch|
          props = AMQ::Protocol::Properties.new(reply_to: "amq.direct.reply-to")
          ch.queue("test")
          e = ch.exchange("", "direct")
          e.publish_confirm("test", "test", props: props)
        end
      end
    ensure
      s.vhosts["/"].delete_queue("amq.direct.reply-to")
      s.vhosts["/"].delete_queue("test")
    end

    it "should be ok to declare reply-to queue to check if consumer is connected" do
      with_channel do |ch|
        resp = ch.queue_declare("amq.direct.reply-to.random")
        resp[:message_count].should eq 0
      end
    ensure
      s.vhosts["/"].delete_queue("amq.direct.reply-to.random")
    end

    it "should return on mandatory publish to a reply routing key" do
      with_channel do |ch|
        ch1 = Channel(Tuple(UInt16, String)).new
        ch.on_return do |msg|
          ch1.send({msg.reply_code, msg.reply_text})
        end
        ch.basic_publish("m1", "amq.direct", "amq.direct.reply-to.random", mandatory: true)
        reply_code, reply_text = ch1.receive
        reply_code.should eq 312
        reply_text.should eq "NO_ROUTE"
      end
    end
  end
end
