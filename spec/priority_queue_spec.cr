require "./spec_helper"

module PrioSpec
  # PriorityMessageStore constructor is protected, so make a subclass
  # with public constructor for testing
  class PrioQueue < LavinMQ::AMQP::PriorityQueue::PriorityMessageStore
    def initialize(*args, **kwargs)
      super(*args, **kwargs)
    end
  end
end

def with_prio_store(max_prio, &)
  data_dir = File.tempname
  Dir.mkdir data_dir
  store = PrioSpec::PrioQueue.new(max_prio.to_u8, data_dir, nil, metadata: ::Log::Metadata.empty)
  yield store
ensure
  store.delete if store
  FileUtils.rm_rf data_dir if data_dir
end

describe LavinMQ::AMQP::PriorityQueue do
  describe "PriorityMessageStore" do
    describe "clustering" do
      add_etcd_around_each
      it "is replicated correctly" do
        with_clustering do |cluster|
          with_amqp_server(replicator: cluster.replicator) do |s|
            with_channel(s) do |ch|
              args = AMQP::Client::Arguments.new({"x-max-priority" => 3})
              q = ch.queue("repli", args: args)
              q.publish_confirm "hello world", props: AMQP::Client::Properties.new(priority: 1_u8)
              q.publish_confirm "hello world 2", props: AMQP::Client::Properties.new(priority: 2_u8)
              if msg = q.get(no_ack: false)
                msg.properties.priority.should eq 2
                msg.ack
              else
                fail("could not get message")
              end
            end
            cluster.stop
          end

          server = LavinMQ::Server.new(cluster.follower_config)
          begin
            q = server.vhosts["/"].queues["repli"].as(LavinMQ::AMQP::DurablePriorityQueue)
            q.message_count.should eq 1
            q.basic_get(true) do |env|
              env.message.properties.priority.should eq 1
              String.new(env.message.body).to_s.should eq "hello world"
            end.should be_true
          ensure
            server.close
          end
        end
      end

      it "can replicate migration" do
        with_clustering do |cluster|
          Dir.mkdir_p cluster.config.data_dir
          old_store = LavinMQ::MessageStore.new(cluster.config.data_dir, cluster.replicator, durable: true)
          60u8.times do |prio|
            props = AMQP::Client::Properties.new(priority: prio % 6)
            msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
            old_store.push msg
          end
          old_store.close

          # Trigger migration
          store = PrioSpec::PrioQueue.new(5u8, cluster.config.data_dir, cluster.replicator, durable: true)
          store.size.should eq 60
          store.close

          cluster.stop

          # Verify the replicated store
          replicated_store = PrioSpec::PrioQueue.new(5u8, cluster.follower_config.data_dir, nil, durable: true)
          replicated_store.size.should eq 60
          6.times do |i|
            replicated_store.@stores[i].size.should eq 10
          end
        ensure
          replicated_store.close if replicated_store
          store.close if store
          old_store.close if old_store
          FileUtils.rm_rf cluster.config.data_dir
        end
      end
    end

    describe "migration" do
      it "should migrate old store" do
        data_dir = File.tempname
        Dir.mkdir data_dir

        old_store = LavinMQ::MessageStore.new(data_dir, nil, durable: true)
        60u8.times do |prio|
          props = AMQP::Client::Properties.new(priority: prio % 6)
          msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
          old_store.push msg
        end
        old_store.close

        store = PrioSpec::PrioQueue.new(5u8, data_dir, nil, durable: true)
        store.size.should eq 60

        6.times do |i|
          store.@stores[i].size.should eq 10
        end

        old_store = LavinMQ::MessageStore.new(data_dir, nil, durable: true)
        old_store.size.should eq 0
        old_store.close
      ensure
        store.delete if store
        FileUtils.rm_rf data_dir if data_dir
      end
    end

    it "should use sub stores" do
      with_prio_store(5) do |store|
        store.@stores.size.should eq 6
      end
    end

    describe "#size" do
      it "is increased on push" do
        with_prio_store(5) do |store|
          100u8.times do |prio|
            props = AMQP::Client::Properties.new(priority: prio % 5)
            msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
            store.push msg
            store.size.should eq(prio + 1)
          end
        end
      end
      it "is decreased on shift?" do
        with_prio_store(5) do |store|
          100u8.times do |prio|
            props = AMQP::Client::Properties.new(priority: prio % 5)
            msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
            store.push msg
          end

          100u8.times do |i|
            store.size.should eq(100 - i)
            store.shift?
          end
          store.size.should eq 0
        end
      end
    end

    describe "#bytesize" do
      it "is increased on push" do
        with_prio_store(5) do |store|
          bytesize = 0
          100u8.times do |prio|
            props = AMQP::Client::Properties.new(priority: prio % 5)
            msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
            store.push msg
            store.bytesize.should be > bytesize
            bytesize = store.bytesize
          end
        end
      end

      it "is decreased on shift?" do
        with_prio_store(5) do |store|
          100u8.times do |prio|
            props = AMQP::Client::Properties.new(priority: prio % 5)
            msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
            store.push msg
          end

          bytesize = store.bytesize
          100u8.times do
            store.shift?
            store.bytesize.should be < bytesize
          end
          store.bytesize.should eq 0
        end
      end
    end

    describe "#push" do
      it "should publish to the right sub store" do
        with_prio_store(5) do |store|
          6u8.times do |prio|
            props = AMQP::Client::Properties.new(priority: prio)
            msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
            store.push msg
            store.size.should eq(prio + 1)
          end
        end
      end

      it "should treat to high priority as max_priority" do
        with_prio_store(5) do |store|
          props = AMQP::Client::Properties.new(priority: 10u8)
          msg = LavinMQ::Message.new("ex", "rk", "body", properties: props)
          store.push msg
          store.@stores[-1].size.should eq 1
        end
      end
    end

    describe "#first?" do
      it "returns nil if store is empty" do
        with_prio_store(5) do |store|
          store.first?.should be_nil
        end
      end

      it "#first? returns the first message with the highest priority" do
        with_prio_store(5) do |store|
          [3u8, 2u8, 5u8, 1u8, 0u8].each do |prio|
            props = AMQP::Client::Properties.new(priority: prio)
            store.push LavinMQ::Message.new("ex", "rk", "body", properties: props)
          end
          env = store.first?.should_not be_nil
          env.message.properties.priority.should eq 5
        end
      end
    end

    describe "#shift?" do
      it "returns nil if store is empty" do
        with_prio_store(5) do |store|
          store.first?.should be_nil
        end
      end

      it "returns the first message with the highest priority" do
        with_prio_store(5) do |store|
          [3u8, 2u8, 5u8, 1u8, 0u8].each do |prio|
            props = AMQP::Client::Properties.new(priority: prio)
            store.push LavinMQ::Message.new("ex", "rk", "body", properties: props)
          end
          env = store.shift?.should_not be_nil
          env.message.properties.priority.should eq 5
          env = store.shift?.should_not be_nil
          env.message.properties.priority.should eq 3
        end
      end
    end

    describe "#requeue" do
      it "reqeueus to the right sub store" do
        with_prio_store(5) do |store|
          [3u8, 2u8, 5u8, 1u8, 0u8].each do |prio|
            props = AMQP::Client::Properties.new(priority: prio)
            store.push LavinMQ::Message.new("ex", "rk", "body", properties: props)
          end
          env_prio_5 = store.shift?.should_not be_nil
          env_prio_3 = store.shift?.should_not be_nil
          sp_prio_5 = env_prio_5.segment_position
          sp_prio_3 = env_prio_3.segment_position
          sp_prio_5.priority.should eq 5
          sp_prio_3.priority.should eq 3

          store.requeue sp_prio_5
          store.requeue sp_prio_3
          store.@stores[-1].size.should eq 1
        end
      end
    end
  end

  it "should prioritize messages" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
        q = ch.queue("", args: q_args)
        q.publish "prio2", props: AMQP::Client::Properties.new(priority: 2)
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio1")
      end
    end
  end

  it "should prioritize messages as 0 if no prio is set" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
        q = ch.queue("", args: q_args)
        q.publish "prio0"
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        q.publish "prio00"
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio1")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio0")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("prio00")
      end
    end
  end

  it "should only accept int32 as priority" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          q_args = AMQP::Client::Arguments.new({"x-max-priority" => "a"})
          ch.queue("", args: q_args)
        end
      end
    end
  end

  it "should only accept priority >= 0" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 0})
        ch.queue("", args: q_args)
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          q_args = AMQP::Client::Arguments.new({"x-max-priority" => -1})
          ch.queue("", args: q_args)
        end
      end
    end
  end

  it "should only accept priority <= 255" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 255})
        ch.queue("", args: q_args)
        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          q_args = AMQP::Client::Arguments.new({"x-max-priority" => 256})
          ch.queue("", args: q_args)
        end
      end
    end
  end

  it "should not set redelivered to true for a non-requeued message" do
    with_amqp_server do |s|
      with_channel(s) do |ch|
        q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
        q = ch.queue("", args: q_args)
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        msg = q.get(no_ack: false)
        msg = msg.should_not be_nil
        msg.redelivered.should eq false
      end
    end
  end

  it "should set redelivered to true for a requeued message" do
    with_amqp_server do |s|
      q_args = AMQP::Client::Arguments.new({"x-max-priority" => 10})
      with_channel(s) do |ch|
        q = ch.queue("q", args: q_args)
        q.publish "prio1", props: AMQP::Client::Properties.new(priority: 1)
        msg = q.get(no_ack: false).should_not be_nil
        msg.reject(requeue: true)
        msg = q.get(no_ack: true).should_not be_nil
        msg.redelivered.should be_true
      end
    end
  end

  context "after restart" do
    it "can restore the priority queue" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          q = ch.queue("pq", args: AMQP::Client::Arguments.new({"x-max-priority": 9}))
          q.publish "m1"
          q.publish "m2", props: AMQP::Client::Properties.new(priority: 9)
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m2"
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m1"
        end
        s.restart
        with_channel(s) do |ch|
          q = ch.queue("pq", args: AMQP::Client::Arguments.new({"x-max-priority": 9}))
          q.publish "m3", props: AMQP::Client::Properties.new(priority: 8)
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m2"
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m3"
          q.get(no_ack: false).try(&.body_io.to_s).should eq "m1"
        end
      end
    end
  end
end
