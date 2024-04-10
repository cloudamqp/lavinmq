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
    it "should pause the queue by setting it in flow (get)" do
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

    it "should pause the queue by setting it in flow (consume)" do
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

  describe "Flow" do
    it "should stop queues from being declared when disk is full" do
      Server.flow(false)
      with_channel do |ch|
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          ch.queue("test_queue_flow", durable: true)
        end
      end
    ensure
      Server.flow(true)
    end
  end

  describe "MessageStore" do
    # Delete unused segments bug
    # https://github.com/cloudamqp/lavinmq/pull/565
    it "should remove unused segments after being consumed" do
      data_dir = File.join(LavinMQ::Config.instance.data_dir, "msgstore")
      Dir.mkdir_p data_dir
      store = LavinMQ::Queue::MessageStore.new(data_dir, nil)
      body = IO::Memory.new(Random::DEFAULT.random_bytes(LavinMQ::Config.instance.segment_size), writeable: false)
      msg = LavinMQ::Message.new(0i64, "amq.topic", "rk", AMQ::Protocol::Properties.new, body.size.to_u64, body)
      sps = Array(LavinMQ::SegmentPosition).new(10) { store.push msg }
      sps.each { |sp| store.delete sp }
      store.@segments.size.should be <= 2
    end

    it "should yield fiber while purging" do
      tmpdir = File.tempname "lavin", ".spec"
      Dir.mkdir_p tmpdir
      store = LavinMQ::Queue::MessageStore.new(tmpdir, nil)

      (LavinMQ::Queue::MessageStore::PURGE_YIELD_INTERVAL * 2 + 1).times do
        store.push(LavinMQ::Message.new(0i64, "a", "b", AMQ::Protocol::Properties.new, 0u64, IO::Memory.new(0)))
      end

      yields = 0
      done = Channel(Nil).new
      spawn(name: "yield counter", same_thread: true) do
        loop do
          select
          when timeout(0.seconds)
            yields += 1
          when done.receive
            break
          end
        end
      end

      spawn(name: "purger", same_thread: true) do
        store.purge
        2.times { done.send nil }
      end

      done.receive

      yields.should eq 2
    ensure
      FileUtils.rm_rf tmpdir if tmpdir
    end
  end
end
