require "./spec_helper"

def with_prio_store(max_prio, &)
  data_dir = File.tempname
  Dir.mkdir data_dir
  store = LavinMQ::AMQP::PriorityQueue::PriorityMessageStore.new(max_prio.to_u8, data_dir, nil, metadata: ::Log::Metadata.empty)
  yield store
ensure
  store.delete if store
  FileUtils.rm_rf data_dir if data_dir
end

describe LavinMQ::AMQP::PriorityQueue do
  describe "PriorityMessageStore" do
    it "should use sub stores" do
      with_prio_store(5) do |store|
        store.@stores.size.should eq 6
      end
    end

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

    describe "#shift?" do
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
          env = store.shift?.should_not be_nil
          store.requeue env.segment_position
          store.@stores[0].size.should eq 1
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
