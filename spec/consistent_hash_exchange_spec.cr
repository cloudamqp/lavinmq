require "./spec_helper"
require "../src/lavinmq/consistent_hasher.cr"

describe "Consistent Hash Exchange" do
  describe "Hasher" do
    # weight/replicas = binding key as INT
    # hash on queue name
    # no-op
    it "should return nil if empty" do
      ch = ConsistentHasher(String).new
      ch.get("1").should be_nil
    end

    it "should return first value is ring size is 1" do
      ch = ConsistentHasher(String).new
      ch.add("first", 1, "first")
      ch.get("any key").should eq "first"
    end

    it "should be consistent (test 1)" do
      ch = ConsistentHasher(String).new
      ch.add("1", 3, "first")
      key = "q1"
      v = ch.get(key)
      ch.add("2", 3, "second")
      ch.get(key).should eq v
      ch.add("3", 3, "third")
      ch.get(key).should eq v
    end

    it "should be consistent (test 2)" do
      # Since replias for this test is 3, each entry get three hashes
      # So 9 Hashes will be stored on the ring
      # Here its listed in alphabetical order and which entry they point to
      #   423 104 025 => 1
      #   448 584 311 => 3
      #   461 287 488 => 2
      # 2 150 719 395 => 1
      # 2 188 894 714 => 2
      # 2 209 713 101 => 3
      # 4 105 354 075 => 3
      # 4 117 811 564 => 2
      # 4 147 539 765 => 1

      # routing keys
      # r1 =>   219 023 793
      # r2 => 2 483 509 259
      # r3 => 3 808 454 813

      ch = ConsistentHasher(String).new
      ch.add("1", 3, "first")
      ch.add("2", 3, "second")
      ch.add("3", 3, "third")

      # 1. Look up the hash for r1
      # 2. Find the hash in the ring that is the closes above in value
      ch.get("r1").should eq "first"
      ch.get("r2").should eq "third"
      ch.get("r3").should eq "third"
    end

    it "should be consistent after delete" do
      ch = ConsistentHasher(String).new
      ch.add("1", 3, "first")
      ch.add("2", 3, "second")
      ch.add("3", 3, "third")

      ch.get("r1").should eq "first"
      ch.get("r2").should eq "third"
      ch.get("r3").should eq "third"

      ch.remove("2", 3)

      ch.get("r1").should eq "first"
      ch.get("r2").should eq "third"
      ch.get("r3").should eq "third"
    end
  end

  describe "exchange => queue bindings" do
    x_name = "con-hash"
    # List of queues to keep track of so we can clean up
    it "should route all messages to queue if only one queue" do
      with_channel do |ch|
        x = ch.exchange(x_name, "x-consistent-hash")
        q = ch.queue("my-queue-0")
        q.bind(x.name, "1")
        x.publish("test message", "rk")
        q.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message")
      end
    end

    it "should route on routing key" do
      q_names = Array(String).new
      with_channel do |ch|
        x = ch.exchange(x_name, "x-consistent-hash")

        q_names << "1"
        q0 = ch.queue(q_names[0])
        q0.bind(x.name, "3")

        q_names << "2"
        q1 = ch.queue(q_names[1])
        q1.bind(x.name, "3")

        q_names << "3"
        q2 = ch.queue(q_names[2])
        q2.bind(x.name, "3")

        x.publish "test message 0", "r1"
        x.publish "test message 1", "r2"
        x.publish "test message 2", "r3"

        q0.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 0")
        q1.get(no_ack: true)
          .try(&.body_io.to_s)
          .should be_nil
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 1")
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 2")
      end
    end

    it "should fail if header value isn't a string" do
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-hash-on" => "cluster"})
        x = ch.exchange(x_name, "x-consistent-hash", args: x_args)
        q0 = ch.queue("1")
        q0.bind(x.name, "3")

        hdrs1 = AMQP::Client::Arguments.new({"cluster" => 123})

        expect_raises(AMQP::Client::Channel::ClosedException, "PRECONDITION_FAILED") do
          x.publish_confirm "test message 0", "abc", props: AMQP::Client::Properties.new(headers: hdrs1)
        end
      end
    end

    it "should route on empty string is header isn't set" do
      q_names = Array(String).new
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-hash-on" => "cluster"})
        x = ch.exchange(x_name, "x-consistent-hash", args: x_args)
        q_names << "1"
        q0 = ch.queue(q_names[0])
        q0.bind(x.name, "3")

        q_names << "2"
        q1 = ch.queue(q_names[1])
        q1.bind(x.name, "3")

        hdrs1 = AMQP::Client::Arguments.new
        hdrs2 = AMQP::Client::Arguments.new({"cluster" => "1"})
        hdrs3 = AMQP::Client::Arguments.new({"cluster" => ""})

        x.publish_confirm "test message 0", "abc", props: AMQP::Client::Properties.new(headers: hdrs1)
        x.publish_confirm "test message 1", "abc", props: AMQP::Client::Properties.new(headers: hdrs2)
        x.publish_confirm "test message 2", "abc", props: AMQP::Client::Properties.new(headers: hdrs3)

        q0.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 0")
        q0.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 2")
        q1.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 1")
      end
    end

    it "should route on header key" do
      q_names = Array(String).new
      with_channel do |ch|
        x_args = AMQP::Client::Arguments.new({"x-hash-on" => "cluster"})
        x = ch.exchange(x_name, "x-consistent-hash", args: x_args)

        q_names << "1"
        q0 = ch.queue(q_names[0])
        q0.bind(x.name, "3")

        q_names << "2"
        q1 = ch.queue(q_names[1])
        q1.bind(x.name, "3")

        q_names << "3"
        q2 = ch.queue(q_names[2])
        q2.bind(x.name, "3")

        hdrs1 = AMQP::Client::Arguments.new({"cluster" => "r1"})
        x.publish "test message 0", "abc", props: AMQP::Client::Properties.new(headers: hdrs1)

        hdrs2 = AMQP::Client::Arguments.new({"cluster" => "r2"})
        x.publish "test message 1", "abc", props: AMQP::Client::Properties.new(headers: hdrs2)

        hdrs3 = AMQP::Client::Arguments.new({"cluster" => "r3"})
        x.publish "test message 2", "abc", props: AMQP::Client::Properties.new(headers: hdrs3)

        q0.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 0")
        q1.get(no_ack: true)
          .try(&.body_io.to_s)
          .should be_nil
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 1")
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 2")
      end
    end

    it "should route on to same queue even after delete" do
      with_channel do |ch|
        x = ch.exchange(x_name, "x-consistent-hash")

        q0 = ch.queue("1")
        q0.bind(x.name, "3")

        q1 = ch.queue("2")
        q1.bind(x.name, "3")

        q2 = ch.queue("3")
        q2.bind(x.name, "3")

        x.publish "test message 0", "r1"
        x.publish "test message 1", "r2"
        x.publish "test message 2", "r3"

        q0.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 0")
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 1")
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 2")

        Server.vhosts["/"].delete_queue("2")

        x.publish "test message 0", "r1"
        x.publish "test message 1", "r2"
        x.publish "test message 2", "r3"

        q0.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 0")
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 1")
        q2.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message 2")
      end
    end
  end

  describe "exchange => exchange bindings" do
    it "should route all messages to exchange if only one exchange" do
      x_name = "con-hash"
      x2_name = "next-exchange"

      with_channel do |ch|
        x = ch.exchange(x_name, "x-consistent-hash")

        x2 = ch.exchange(x2_name, "direct")
        x2.bind(x_name, "1")

        q = ch.queue("my-queue")
        q.bind(x2_name, "rk")

        x.publish("test message", "rk")
        q.get(no_ack: true)
          .try(&.body_io.to_s)
          .should eq("test message")
      end
    end
  end
end
