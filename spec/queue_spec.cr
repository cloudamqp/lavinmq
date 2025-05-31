require "./spec_helper"
require "./../src/lavinmq/amqp/queue"

describe LavinMQ::AMQP::Queue do
  it "should expire it self after last consumer disconnects" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("qexpires", args: AMQP::Client::Arguments.new({"x-expires" => 100}))
        queue = s.vhosts["/"].queues["qexpires"]
        tag = q.subscribe { }
        sleep 110.milliseconds
        queue.closed?.should be_false
        ch.basic_cancel(tag)
        start = Time.monotonic
        should_eventually(be_true) { queue.closed? }
        (Time.monotonic - start).total_milliseconds.should be_close 100, 50
      end
    end
  end

  it "Should dead letter expired messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
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
  end

  it "Should not dead letter messages to it self due to queue length" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q1 = ch.queue("", args: AMQP::Client::Arguments.new(
          {"x-max-length" => 1, "x-dead-letter-exchange" => ""}
        ))
        q1.publish_confirm ""
        q1.publish_confirm ""
        q1.get.should_not be_nil
        q1.get.should be_nil
      end
    end
  end

  it "Should dead letter messages to it self only if rejected" do
    queue_name = Random::Secure.hex
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q1 = ch.queue(queue_name, args: AMQP::Client::Arguments.new(
          {"x-dead-letter-exchange" => ""}
        ))
        ch.default_exchange.publish_confirm("", queue_name)
        msg = q1.get(no_ack: false).not_nil!
        msg.reject(requeue: false)
        q1.get(no_ack: false).should_not be_nil
      end
    end
  end

  describe "Paused" do
    x_name = "paused"
    q_name = "paused"
    it "should pause the queue by setting it in flow (get)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "direct")
          q = ch.queue(q_name)
          q.bind(x.name, q.name)
          x.publish_confirm "test message", q.name
          q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

          iq = s.vhosts["/"].queues[q.name]
          iq.pause!

          x.publish_confirm "test message 2", q.name
          q.get(no_ack: true).should be_nil
        end
      end
    end

    it "should be able to declare queue as paused" do
      with_amqp_server do |s|
        s.vhosts.create("/")
        v = s.vhosts["/"].not_nil!
        v.declare_queue("q", true, false)
        data_dir = s.vhosts["/"].queues["q"].as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
        s.vhosts["/"].queues["q"].pause!
        File.exists?(File.join(data_dir, ".paused")).should be_true
        s.restart
        s.vhosts["/"].queues["q"].state.paused?.should be_true
        s.vhosts["/"].queues["q"].resume!
        File.exists?(File.join(data_dir, ".paused")).should be_false
      end
    end

    it "should pause the queue by setting it in flow (consume)" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "direct")
          q = ch.queue(q_name)
          q.bind(x.name, q.name)

          x.publish_confirm "test message", q.name
          q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

          iq = s.vhosts["/"].queues[q.name]
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
    end

    it "should be able to get messages from paused queue with force flag" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "direct")
          q = ch.queue(q_name)
          q.bind(x.name, q.name)
          x.publish_confirm "test message", q.name
          q.get(no_ack: true).try(&.body_io.to_s).should eq("test message")

          iq = s.vhosts["/"].queues[q.name]
          iq.pause!

          x.publish_confirm "test message 2", q.name
          x.publish_confirm "test message 3", q.name

          # cannot get from client, queue is paused
          q.get(no_ack: true).should be_nil

          body = %({ "count": 1, "ack_mode": "get", "encoding": "auto" })
          response = http.post("/api/queues/%2f/#{q_name}/get", body: body)
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
  end

  describe "Close" do
    q_name = "close"
    it "should cancel consumer" do
      with_amqp_server do |s|
        tag = "consumer-to-be-canceled"
        with_channel(s) do |ch|
          q = ch.queue(q_name)
          queue = s.vhosts["/"].queues[q_name].as(LavinMQ::AMQP::DurableQueue)
          q.publish_confirm "m1"

          # Should get canceled
          q.subscribe(tag: tag, no_ack: false, &.ack)
          queue.close
        end

        with_channel(s) do |ch|
          ch.has_subscriber?(tag).should eq false
        end
      end
    end
  end

  describe "Purge" do
    x_name = "purge"
    q_name = "purge"
    it "should purge the queue" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "direct")
          q = ch.queue(q_name, durable: true)
          q.bind(x.name, q.name)
          x.publish_confirm "test message 1", q.name
          x.publish_confirm "test message 2", q.name
          x.publish_confirm "test message 3", q.name
          x.publish_confirm "test message 4", q.name

          internal_queue = s.vhosts["/"].queues[q.name]
          internal_queue.message_count.should eq 4

          response = http.delete("/api/queues/%2f/#{q_name}/contents")
          response.status_code.should eq 204

          internal_queue.message_count.should eq 0
        end
      end
    end

    it "should purge only X messages from queue" do
      with_http_server do |http, s|
        with_channel(s) do |ch|
          x = ch.exchange(x_name, "direct")
          q = ch.queue(q_name, durable: true)
          q.bind(x.name, q.name)
          10.times do |i|
            x.publish_confirm "test message #{i}", q.name
          end

          vhost = s.vhosts["/"]
          internal_queue = vhost.queues[q.name]
          internal_queue.message_count.should eq 10

          response = http.delete("/api/queues/%2f/#{q_name}/contents?count=5")
          response.status_code.should eq 204

          internal_queue.message_count.should eq 5
        end
      end
    end
  end

  it "should keep track of unacked basic_get messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        q.publish_confirm "a"

        msg = q.get(no_ack: false)
        msg.should_not be_nil
        sq = s.vhosts["/"].queues[q.name]
        sq.unacked_count.should eq 1
        msg.not_nil!.ack
        sleep 10.milliseconds
        sq.unacked_count.should eq 0
      end
    end
  end

  it "should keep track of unacked deliviered messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue
        q.publish_confirm "a"

        done = Channel(AMQP::Client::DeliverMessage).new
        q.subscribe(no_ack: false) do |msg|
          done.send msg
        end
        msg = done.receive
        sq = s.vhosts["/"].queues[q.name]
        sq.unacked_count.should eq 1
        msg.ack
        sleep 10.milliseconds
        sq.unacked_count.should eq 0
      end
    end
  end

  it "should delete transient queues on Server stop" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue "transient", durable: false
      end
      data_dir = s.vhosts["/"].queues["transient"].as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
      s.stop
      Dir.exists?(data_dir).should be_false
    end
  end

  it "should delete transient queues segments on creation" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        ch.queue "transient", durable: false
      end
      data_dir = s.vhosts["/"].queues["transient"].as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
      Dir.exists?(data_dir).should be_true
      File.exists?("#{data_dir}/msgs.0000000001").should be_false
    end
  end

  it "should delete left over transient queue data on Server start" do
    with_amqp_server do |s|
      data_dir = ""
      with_channel(s) do |ch|
        q = ch.queue "transient", durable: false
        q.publish_confirm "foobar"
        data_dir = s.vhosts["/"].queues["transient"].as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
        FileUtils.cp_r data_dir, "#{s.vhosts["/"].data_dir}.copy"
      end
      s.stop
      Dir.mkdir_p data_dir
      FileUtils.cp_r "#{s.vhosts["/"].data_dir}.copy", data_dir
      s.restart
      with_channel(s) do |ch|
        q = ch.queue_declare "transient", durable: false
        q[:message_count].should eq 0
        q = ch.queue_declare "transient", passive: true
        q[:message_count].should eq 0
      end
    end
  end

  it "should delete queue with auto_delete when the last consumer disconnects" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q = ch.queue("q", auto_delete: true)
        data_dir = s.vhosts["/"].queues["q"].as(LavinMQ::AMQP::Queue).@msg_store.@msg_dir
        sub = q.subscribe(no_ack: true) { |_| }
        Dir.exists?(data_dir).should be_true
        q.unsubscribe(sub)
        sleep 0.1.seconds
        Dir.exists?(data_dir).should be_false
      end
    end
  end

  describe "Flow" do
    it "should stop queues from being declared when disk is full" do
      with_amqp_server do |s|
        s.flow(false)
        with_channel(s) do |ch|
          expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
            ch.queue("test_queue_flow", durable: true)
          end
        end
      end
    end
  end

  describe "MessageStore" do
    # Delete unused segments bug
    # https://github.com/cloudamqp/lavinmq/pull/565
    it "should remove unused segments after being consumed" do
      data_dir = File.join(LavinMQ::Config.instance.data_dir, "msgstore")
      Dir.mkdir_p data_dir
      begin
        store = LavinMQ::Queue::MessageStore.new(data_dir, nil)
        body = IO::Memory.new(Random::DEFAULT.random_bytes(LavinMQ::Config.instance.segment_size), writeable: false)
        msg = LavinMQ::Message.new(0i64, "amq.topic", "rk", AMQ::Protocol::Properties.new, body.size.to_u64, body)
        sps = Array(LavinMQ::SegmentPosition).new(10) { store.push msg }
        sps.each { |sp| store.delete sp }
        Fiber.yield
        store.@segments.size.should be <= 2
      ensure
        FileUtils.rm_rf data_dir
      end
    end

    it "should not create ack files when cleaning up segments" do
      # This spec verifies a bugfix where one ack file per segment was created
      data_dir = File.join(LavinMQ::Config.instance.data_dir, "msgstore2")
      Dir.mkdir_p data_dir
      begin
        body = IO::Memory.new(Random::DEFAULT.random_bytes(LavinMQ::Config.instance.segment_size), writeable: false)
        msg = LavinMQ::Message.new(0i64, "amq.topic", "rk", AMQ::Protocol::Properties.new, body.size.to_u64, body)

        store = LavinMQ::Queue::MessageStore.new(data_dir, nil)
        2.times { store.push msg }
        store.close

        # recreate store to let it read the segments and cleanup
        LavinMQ::Queue::MessageStore.new(data_dir, nil)
        Dir.glob(File.join(data_dir, "acks.*")).should eq [] of String
      ensure
        FileUtils.rm_rf data_dir
      end
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

    it "should not raise NotFoundError if segment is gone when deleting" do
      tmpdir = File.tempname "lavin", ".spec"
      Dir.mkdir_p tmpdir
      store = LavinMQ::Queue::MessageStore.new(tmpdir, nil)
      data = Random::Secure.hex(512)
      io = IO::Memory.new(data.to_slice)

      # Publish enough data to have more than one segment
      until store.@segments.size > 1
        io.rewind
        store.push(LavinMQ::Message.new(0i64, "a", "b", AMQ::Protocol::Properties.new, data.bytesize.to_u64, io))
      end

      Dir.glob(File.join(tmpdir, "msgs.*")).each do |f|
        File.delete(f)
      end

      store.purge
    ensure
      FileUtils.rm_rf tmpdir if tmpdir
    end
  end

  describe "deduplication" do
    it "should not except message if it's a duplicate" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          queue_name = "dedup-queue"
          q1 = ch.queue(queue_name, args: AMQP::Client::Arguments.new({
            "x-message-deduplication" => true,
          }))
          props = LavinMQ::AMQP::Properties.new(headers: LavinMQ::AMQP::Table.new({
            "x-deduplication-header" => "msg1",
          }))
          ch.default_exchange.publish_confirm("body", queue_name, props: props)
          ch.default_exchange.publish_confirm("body", queue_name, props: props)
          q1.get(no_ack: false).not_nil!.body_io.to_s.should eq "body"
          q1.get(no_ack: false).should be_nil
        end
      end
    end
  end
end
