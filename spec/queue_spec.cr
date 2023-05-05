require "./spec_helper"
require "./../src/lavinmq/queue"

describe LavinMQ::Queue do
  it "Should dead letter expired messages" do
    with_channel do |ch|
      q = ch.queue("ttl", args: AMQP::Client::Arguments.new(
        {"x-message-ttl" => 1, "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "dlq"}
      ))
      dlq = ch.queue("dlq")
      x = ch.default_exchange
      x.publish_confirm("ttl", q.name)
      msg = wait_for { dlq.get }
      msg.not_nil!.body_io.to_s.should eq "ttl"
      q.get.should eq nil
    end
  end

  it "Should not dead letter messages to it self due to queue length" do
    with_channel do |ch|
      q1 = ch.queue("", args: AMQP::Client::Arguments.new(
        {"x-max-length" => 1, "x-dead-letter-exchange" => ""}
      ))
      q1.publish_confirm ""
      q1.publish_confirm ""
      q1.get.should_not be_nil
      q1.get.should be_nil
    end
  end

  it "Should dead letter messages to it self only if rejected" do
    queue_name = Random::Secure.hex
    with_channel do |ch|
      q1 = ch.queue(queue_name, args: AMQP::Client::Arguments.new(
        {"x-dead-letter-exchange" => ""}
      ))
      ch.default_exchange.publish_confirm("", queue_name)
      msg = q1.get(no_ack: false).not_nil!
      msg.reject(requeue: false)
      q1.get(no_ack: false).should_not be_nil
    end
  end

  describe "Paused" do
    x_name = "paused"
    q_name = "paused"
    it "should paused the queue by setting it in flow (get)" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name)
        q.bind(x.name, q.name)
        x.publish_confirm "test message", q.name
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

        iq = Server.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        iq.pause!

        x.publish_confirm "test message 2", q.name
        q.get(no_ack: true).should be_nil
      end
    end

    it "should be able to declare queue as paused" do
      Server.vhosts.create("/")
      v = Server.vhosts["/"].not_nil!
      v.declare_queue("q", true, false)
      data_dir = Server.vhosts["/"].queues["q"].@msg_store.@data_dir
      Server.vhosts["/"].queues["q"].pause!
      File.exists?(File.join(data_dir, ".paused")).should be_true
      Server.restart
      Server.vhosts["/"].queues["q"].state.paused?.should be_true
      Server.vhosts["/"].queues["q"].resume!
      File.exists?(File.join(data_dir, ".paused")).should be_false
    end


    it "should paused the queue by setting it in flow (consume)" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name)
        q.bind(x.name, q.name)

        x.publish_confirm "test message", q.name
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

        iq = Server.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        iq.pause!

        x.publish_confirm "test message 2", q.name
        channel = Channel(String).new

        # Subscribe on the queue
        # Wait 1 second and unpause the queue, fail test if we get message during that time
        # Make sure the queue continues
        q.subscribe(no_ack: false) do |msg|
          channel.send msg.body_io.to_s
          ch.basic_ack(msg.delivery_tag)
        end
        select
        when msg = channel.receive
          fail "Consumer should not get a message '#{msg}'"
        when timeout 2.seconds
          iq.resume!
        end
        channel.receive.should eq "test message 2"
      end
    end

    it "should be able to get messages from paused queue with force flag" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name)
        q.bind(x.name, q.name)
        x.publish_confirm "test message", q.name
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

        iq = Server.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        iq.pause!

        x.publish_confirm "test message 2", q.name
        x.publish_confirm "test message 3", q.name

        # cannot get from client, queue is paused
        q.get(no_ack: true).should be_nil

        body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
        response = post("/api/queues/%2f/#{q_name}/get", body: body)
        response.status_code.should eq 200
        body = JSON.parse(response.body)
        body.size.should eq 1
        # can get from UI/API even though queue is paused
        body[0]["payload"].should eq "test message 2"

        # resume queue and consume next msg
        iq.resume!
        q.get(no_ack: true).try(&.body_io.to_s).should eq("test message 3")
      end
    end
  end

  describe "Close" do
    q_name = "close"
    it "should cancel consumer" do
      tag = "consumer-to-be-canceled"
      with_channel do |ch|
        q = ch.queue(q_name)
        queue = Server.vhosts["/"].queues[q_name].as(LavinMQ::DurableQueue)
        q.publish_confirm "m1"

        # Should get canceled
        q.subscribe(tag: tag, no_ack: false, &.ack)
        queue.close
      end

      with_channel do |ch|
        ch.has_subscriber?(tag).should eq false
      end
    end
  end

  describe "Purge" do
    x_name = "purge"
    q_name = "purge"
    it "should purge the queue" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name, durable: true)
        q.bind(x.name, q.name)
        x.publish_confirm "test message 1", q.name
        x.publish_confirm "test message 2", q.name
        x.publish_confirm "test message 3", q.name
        x.publish_confirm "test message 4", q.name

        internal_queue = Server.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first
        internal_queue.message_count.should eq 4

        response = delete("/api/queues/%2f/#{q_name}/contents")
        response.status_code.should eq 204

        internal_queue.message_count.should eq 0
      end
    end

    it "should purge only X messages from queue" do
      with_channel do |ch|
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name, durable: true)
        q.bind(x.name, q.name)
        10.times do |i|
          x.publish_confirm "test message #{i}", q.name
        end

        vhost = Server.vhosts["/"]
        internal_queue = vhost.exchanges[x_name].queue_bindings[{q.name, nil}].first
        internal_queue.message_count.should eq 10

        response = delete("/api/queues/%2f/#{q_name}/contents?count=5")
        response.status_code.should eq 204

        internal_queue.message_count.should eq 5
      end
    end
  end

  describe "#purge_and_close_consumers" do
    x_name = "purge_and_close"
    q_name = "purge_and_close"
    it "should cancel all consumers on the queue" do
      with_channel do |ch|
        ch.prefetch 5
        x = ch.exchange(x_name, "direct")
        q = ch.queue(q_name, durable: true)
        q.bind(x.name, q.name)
        10.times do |i|
          x.publish_confirm "test message #{i}", q.name
        end

        internal_queue = Server.vhosts["/"].exchanges[x_name].queue_bindings[{q.name, nil}].first

        internal_queue.message_count.should eq 10

        channel = Channel(String).new(1)
        # Get one message of the queue
        q.subscribe(no_ack: false) do |msg|
          channel.send msg.body_io.to_s
          ch.basic_ack(msg.delivery_tag)
        end

        # 10 messages in total, 5 unacked and 5 ready
        internal_queue.message_count.should eq 5

        # Here we have one active consumer with 5 unacked messages.
        internal_queue.consumers.first.unacked.should eq 5

        # Purge should remove all of those
        internal_queue.purge_and_close_consumers

        # No messages left in the queue
        internal_queue.message_count.should eq 0

        # No consumers lest on the queue and therefore no unacked
        internal_queue.consumers.empty?.should be_true
      end
    end
  end

  it "should keep track of unacked basic_get messages" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "a"

      msg = q.get(no_ack: false)
      msg.should_not be_nil
      sq = Server.vhosts["/"].queues[q.name]
      sq.unacked_count.should eq 1
      msg.not_nil!.ack
      sleep 0.01
      sq.unacked_count.should eq 0
    end
  end

  it "should keep track of unacked deliviered messages" do
    with_channel do |ch|
      q = ch.queue
      q.publish_confirm "a"

      done = Channel(AMQP::Client::DeliverMessage).new
      q.subscribe(no_ack: false) do |msg|
        done.send msg
      end
      msg = done.receive
      sq = Server.vhosts["/"].queues[q.name]
      sq.unacked_count.should eq 1
      msg.ack
      sleep 0.01
      sq.unacked_count.should eq 0
    end
  end

  it "should delete transient queues on Server stop" do
    with_channel do |ch|
      ch.queue "transient", durable: false
    end
    data_dir = Server.vhosts["/"].queues["transient"].@msg_store.@data_dir
    Server.stop
    Dir.exists?(data_dir).should be_false
  end

  it "should delete left over transient queue data on Server start" do
    data_dir = ""
    with_channel do |ch|
      q = ch.queue "transient", durable: false
      q.publish_confirm "foobar"
      data_dir = Server.vhosts["/"].queues["transient"].@msg_store.@data_dir
      FileUtils.cp_r data_dir, "#{data_dir}.copy"
    end
    Server.stop
    FileUtils.cp_r "#{data_dir}.copy", data_dir
    Server.restart
    with_channel do |ch|
      q = ch.queue_declare "transient", durable: false
      q[:message_count].should eq 0
      q = ch.queue_declare "transient", passive: true
      q[:message_count].should eq 0
    end
  end

  it "should delete queue with auto_delete when the last consumer disconnects" do
    with_channel do |ch|
      q = ch.queue("q", auto_delete: true)
      data_dir = Server.vhosts["/"].queues["q"].@msg_store.@data_dir
      sub = q.subscribe(no_ack: true) { |_| }
      Dir.exists?(data_dir).should be_true
      q.unsubscribe(sub)
      sleep 0.1
      Dir.exists?(data_dir).should be_false
    end
  end
end
