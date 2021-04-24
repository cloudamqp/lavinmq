require "./spec_helper"

describe AvalancheMQ::VHost do
  vhost = s.vhosts.create("add_policy")
  definitions = {
    "max-length"         => JSON::Any.new(10_i64),
    "alternate-exchange" => JSON::Any.new("dead-letters"),
  } of String => JSON::Any

  it "should be able to add policy" do
    vhost.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    vhost.policies.size.should eq 1
    vhost.delete_policy("test")
  end

  it "should remove policy from resource when deleted" do
    vhost.queues["test1"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    sleep 0.01
    vhost.queues["test1"].policy.try(&.name).should eq "test"
    vhost.delete_policy("test")
    sleep 0.01
    vhost.queues["test1"].policy.should be_nil
  end

  it "should be able to list policies" do
    vhost2 = s.vhosts.create("add_remove_policy")
    vhost2.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    vhost2.delete_policy("test")
    vhost2.policies.size.should eq 0
    s.vhosts.delete("add_remove_policy")
  end

  it "should overwrite policy with same name" do
    vhost.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    vhost.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::Exchanges, definitions, 10_i8)
    vhost.policies.size.should eq 1
    vhost.policies["test"].apply_to.should eq AvalancheMQ::Policy::Target::Exchanges
    vhost.delete_policy("test")
  end

  it "should apply policy" do
    definitions = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
    vhost.queues["test"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 11_i8)
    sleep 0.01
    vhost.queues["test"].policy.not_nil!.name.should eq "ml"
  ensure
    vhost.delete_policy("ml")
  end

  it "should respect priority" do
    definitions = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
    vhost.queues["test2"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml2", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 1_i8)
    vhost.add_policy("ml1", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 0_i8)
    sleep 0.01
    vhost.queues["test2"].policy.not_nil!.name.should eq "ml2"
  ensure
    vhost.delete_policy("ml2")
    vhost.delete_policy("ml1")
  end

  it "should remove effect of deleted policy" do
    definitions = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
    s.vhosts["/"].add_policy("mld", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
    with_channel do |ch|
      q = ch.queue("mld")
      11.times do
        q.publish_confirm "body"
      end
      ch.queue_declare("mld", passive: true)[:message_count].should eq 10
      s.vhosts["/"].delete_policy("mld")
      q.publish_confirm "body"
      ch.queue_declare("mld", passive: true)[:message_count].should eq 11
    end
  ensure
    s.vhosts["/"].delete_policy("mld")
    vhost.delete_queue("mld")
  end

  it "should apply message TTL policy on existing queue" do
    definitions = {"message-ttl" => JSON::Any.new(0_i64)} of String => JSON::Any
    with_channel do |ch|
      q = ch.queue("policy-ttl")
      10.times do
        q.publish_confirm "body"
      end
      ch.queue_declare("policy-ttl", passive: true)[:message_count].should eq 10
      s.vhosts["/"].add_policy("ttl", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
      sleep 0.01
      ch.queue_declare("policy-ttl", passive: true)[:message_count].should eq 0
      s.vhosts["/"].delete_policy("ttl")
    end
  ensure
    s.vhosts["/"].delete_policy("ttl")
    vhost.delete_queue("policy-ttl")
  end

  it "should apply queue TTL policy on existing queue" do
    definitions = {"expires" => JSON::Any.new(0_i64)} of String => JSON::Any
    with_channel do |ch|
      q = ch.queue("qttl")
      q.publish_confirm ""
      s.vhosts["/"].add_policy("qttl", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
      sleep 0.01
      expect_raises(AMQP::Client::Channel::ClosedException) do
        ch.queue_declare("qttl", passive: true)
      end
    end
  ensure
    s.vhosts["/"].delete_policy("qttl")
    vhost.delete_queue("qttl")
  end

  it "should refresh queue last_get_time when expire policy applied" do
    definitions = {"expires" => JSON::Any.new(50_i64)} of String => JSON::Any
    with_channel do |ch|
      ch.queue("qttl")
      queue = s.vhosts["/"].queues["qttl"]
      first = queue.last_get_time
      s.vhosts["/"].add_policy("qttl", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
      sleep 0.01
      last = queue.last_get_time
      (last > first).should be_true
    ensure
      s.vhosts["/"].delete_policy("qttl")
      vhost.delete_queue("qttl")
    end
  end

  it "should apply max-length-bytes on existing queue" do
    definitions = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
    with_channel do |ch|
      q = ch.queue("max-length-bytes", exclusive: true)
      q.publish_confirm "short1"
      q.publish_confirm "short2"
      q.publish_confirm "long"
      ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 3
      sleep 0.02
      s.vhosts["/"].add_policy("max-length-bytes", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
      sleep 0.01
      ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
      q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
      q.get(no_ack: true).try(&.body_io.to_s).should eq("long")
      s.vhosts["/"].delete_policy("max-length-bytes")
    end
  ensure
    s.vhosts["/"].delete_policy("max-length-bytes")
    vhost.delete_queue("max-length-bytes")
  end

  it "should remove head if queue to large" do
    definitions = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
    with_channel do |ch|
      q = ch.queue("max-length-bytes", exclusive: true)
      s.vhosts["/"].add_policy("max-length-bytes", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
      sleep 0.01
      q.publish_confirm "short1"
      q.publish_confirm "short2"
      q.publish_confirm "long"
      ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
      q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
      q.get(no_ack: true).try(&.body_io.to_s).should eq("long")
      s.vhosts["/"].delete_policy("max-length-bytes")
    end
  ensure
    s.vhosts["/"].delete_policy("max-length-bytes")
    vhost.delete_queue("max-length-bytes")
  end

  it "should not enqueue messages that make the queue to large" do
    definitions = {"max-length-bytes" => JSON::Any.new(100_i64),
                   "overflow":           JSON::Any.new("reject-publish")} of String => JSON::Any
    with_channel do |ch|
      q = ch.queue("max-length-bytes", exclusive: true)
      s.vhosts["/"].add_policy("max-length-bytes", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
      sleep 0.01
      q.publish_confirm "short1"
      q.publish_confirm "short2"
      q.publish_confirm "long"
      ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
      q.get(no_ack: true).try(&.body_io.to_s).should eq("short1")
      q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
      s.vhosts["/"].delete_policy("max-length-bytes")
    end
  ensure
    s.vhosts["/"].delete_policy("max-length-bytes")
    vhost.delete_queue("max-length-bytes")
  end

  describe "with max-length-bytes policy applied" do
    it "should replace with max-length" do
      definitions = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
      with_channel do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        s.vhosts["/"].add_policy("max-length-bytes", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 12_i8)
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2

        definitions = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
        s.vhosts["/"].add_policy("max-length-bytes", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 12_i8)
        10.times do
          q.publish_confirm "msg"
        end
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 10
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    ensure
      s.vhosts["/"].delete_policy("max-length-bytes")
      vhost.delete_queue("max-length-bytes")
    end
  end
end
