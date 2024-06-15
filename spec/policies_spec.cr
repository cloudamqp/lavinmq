require "./spec_helper"

class PoliciesSpec
  def self.with_vhost(&)
    with_amqp_server do |s|
      vhost = s.vhosts.create("add_policy")
      yield vhost
    end
  end
end

describe LavinMQ::VHost do
  definitions = {
    "max-length"         => JSON::Any.new(10_i64),
    "alternate-exchange" => JSON::Any.new("dead-letters"),
    "unsupported"        => JSON::Any.new("unsupported"),
  }

  it "should be able to add policy" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      vhost.policies.size.should eq 1
      vhost.delete_policy("test")
    end
  end

  it "should remove policy from resource when deleted" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.queues["test1"] = LavinMQ::Queue.new(vhost, "test")
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      sleep 0.01
      vhost.queues["test1"].policy.try(&.name).should eq "test"
      vhost.delete_policy("test")
      sleep 0.01
      vhost.queues["test1"].policy.should be_nil
    end
  end

  it "should be able to list policies" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      vhost.delete_policy("test")
      vhost.policies.size.should eq 0
    end
  end

  it "should overwrite policy with same name" do
    PoliciesSpec.with_vhost do |vhost|
      vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
      vhost.add_policy("test", "^.*$", "exchanges", definitions, 10_i8)
      vhost.policies.size.should eq 1
      vhost.policies["test"].apply_to.should eq LavinMQ::Policy::Target::Exchanges
      vhost.delete_policy("test")
    end
  end

  it "should apply policy" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
      vhost.queues["test"] = LavinMQ::Queue.new(vhost, "test")
      vhost.add_policy("ml", "^.*$", "queues", defs, 11_i8)
      sleep 0.01
      vhost.queues["test"].policy.not_nil!.name.should eq "ml"
    end
  end

  it "should respect priority" do
    PoliciesSpec.with_vhost do |vhost|
      defs = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
      vhost.queues["test2"] = LavinMQ::Queue.new(vhost, "test")
      vhost.add_policy("ml2", "^.*$", "queues", defs, 1_i8)
      vhost.add_policy("ml1", "^.*$", "queues", defs, 0_i8)
      sleep 0.01
      vhost.queues["test2"].policy.not_nil!.name.should eq "ml2"
    end
  end

  it "should remove effect of deleted policy" do
    with_amqp_server do |s|
      defs = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
      s.vhosts["/"].add_policy("mld", "^.*$", "all", defs, 12_i8)
      with_channel(s) do |ch|
        q = ch.queue("mld")
        11.times do
          q.publish_confirm "body"
        end
        ch.queue_declare("mld", passive: true)[:message_count].should eq 10
        s.vhosts["/"].delete_policy("mld")
        q.publish_confirm "body"
        ch.queue_declare("mld", passive: true)[:message_count].should eq 11
      end
    end
  end

  it "should apply message TTL policy on existing queue" do
    with_amqp_server do |s|
      defs = {"message-ttl" => JSON::Any.new(0_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("policy-ttl")
        10.times do
          q.publish_confirm "body"
        end
        ch.queue_declare("policy-ttl", passive: true)[:message_count].should eq 10
        s.vhosts["/"].add_policy("ttl", "^.*$", "all", defs, 12_i8)
        sleep 0.01
        ch.queue_declare("policy-ttl", passive: true)[:message_count].should eq 0
        s.vhosts["/"].delete_policy("ttl")
      end
    end
  end

  it "should apply queue TTL policy on existing queue" do
    with_amqp_server do |s|
      defs = {"expires" => JSON::Any.new(0_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("qttl")
        q.publish_confirm ""
        s.vhosts["/"].add_policy("qttl", "^.*$", "all", defs, 12_i8)
        sleep 0.01
        expect_raises(AMQP::Client::Channel::ClosedException) do
          ch.queue_declare("qttl", passive: true)
        end
      end
    end
  end

  it "should refresh queue last_get_time when expire policy applied" do
    with_amqp_server do |s|
      defs = {"expires" => JSON::Any.new(50_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        ch.queue("qttl")
        queue = s.vhosts["/"].queues["qttl"]
        first = queue.last_get_time
        sleep 0.1
        s.vhosts["/"].add_policy("qttl", "^.*$", "all", defs, 12_i8)
        sleep 0.1
        last = queue.last_get_time
        last.should be > first
      end
    end
  end

  it "should apply max-length-bytes on existing queue" do
    with_amqp_server do |s|
      defs = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 3
        sleep 0.02
        s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "all", defs, 12_i8)
        sleep 0.01
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("long")
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    end
  end

  it "should remove head if queue to large" do
    with_amqp_server do |s|
      defs = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "all", defs, 12_i8)
        sleep 0.01
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("long")
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    end
  end

  it "should not enqueue messages that make the queue to large" do
    with_amqp_server do |s|
      defs = {"max-length-bytes" => JSON::Any.new(100_i64),
              "overflow"         => JSON::Any.new("reject-publish")} of String => JSON::Any
      with_channel(s) do |ch|
        q = ch.queue("max-length-bytes", exclusive: true)
        s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "all", defs, 12_i8)
        sleep 0.01
        q.publish_confirm "short1"
        q.publish_confirm "short2"
        q.publish_confirm "long"
        ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short1")
        q.get(no_ack: true).try(&.body_io.to_s).should eq("short2")
        s.vhosts["/"].delete_policy("max-length-bytes")
      end
    end
  end

  describe "with max-length-bytes policy applied" do
    it "should replace with max-length" do
      with_amqp_server do |s|
        defs = {"max-length-bytes" => JSON::Any.new(100_i64)} of String => JSON::Any
        with_channel(s) do |ch|
          q = ch.queue("max-length-bytes", exclusive: true)
          s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "queues", defs, 12_i8)
          q.publish_confirm "short1"
          q.publish_confirm "short2"
          q.publish_confirm "long"
          ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 2

          defs = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
          s.vhosts["/"].add_policy("max-length-bytes", "^.*$", "queues", defs, 12_i8)
          10.times do
            q.publish_confirm "msg"
          end
          ch.queue_declare("max-length-bytes", passive: true)[:message_count].should eq 10
          s.vhosts["/"].delete_policy("max-length-bytes")
        end
      end
    end
  end

  describe "operator policies" do
    it "merges with normal polices" do
      with_amqp_server do |s|
        ml_2 = {"max-length" => JSON::Any.new(2_i64)} of String => JSON::Any
        ml_1 = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
        s.vhosts["/"].add_policy("ml", ".*", "all", ml_2, 0_i8)
        with_channel(s) do |ch|
          q = ch.queue
          3.times do
            q.publish_confirm "body"
          end
          ch.queue_declare(q.name, passive: true)[:message_count].should eq 2
          s.vhosts["/"].add_operator_policy("ml1", ".*", "all", ml_1, 0_i8)
          ch.queue_declare(q.name, passive: true)[:message_count].should eq 1

          # deleting operator policy should make normal policy active again
          s.vhosts["/"].delete_operator_policy("ml1")
          3.times do
            q.publish_confirm "body"
          end
          ch.queue_declare(q.name, passive: true)[:message_count].should eq 2
        end
      end
    end

    it "effective_policy_definition should not include unsupported policies" do
      PoliciesSpec.with_vhost do |vhost|
        supported_policies = {"max-length", "max-length-bytes",
                              "message-ttl", "expires", "overflow",
                              "dead-letter-exchange", "dead-letter-routing-key",
                              "federation-upstream", "federation-upstream-set",
                              "delivery-limit", "max-age", "alternate-exchange",
                              "delayed-message"}
        vhost.queues["test"] = LavinMQ::Queue.new(vhost, "test")
        vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
        sleep 0.01
        vhost.queues["test"].details_tuple[:effective_policy_definition].as(Hash(String, JSON::Any)).each_key do |k|
          supported_policies.includes?(k).should be_true
        end
        vhost.delete_policy("test")
      end
    end
  end

  describe "together with arguments" do
    it "arguments should have priority for non numeric arguments" do
      PoliciesSpec.with_vhost do |vhost|
        vhost.exchanges["no-ae"] = LavinMQ::DirectExchange.new(vhost, "no-ae")
        vhost.exchanges["x-with-ae"] = LavinMQ::DirectExchange.new(vhost, "x-with-ae",
          arguments: AMQ::Protocol::Table.new({"x-alternate-exchange": "ae2"}))
        vhost.add_policy("test", ".*", "all", definitions, 100_i8)
        sleep 0.01
        vhost.exchanges["no-ae"].@alternate_exchange.should eq "dead-letters"
        vhost.exchanges["x-with-ae"].@alternate_exchange.should eq "ae2"
        vhost.delete_policy("test")
        sleep 0.01
        vhost.exchanges["no-ae"].@alternate_exchange.should be_nil
        vhost.exchanges["x-with-ae"].@alternate_exchange.should eq "ae2"
      end
    end

    it "should use the lowest value" do
      PoliciesSpec.with_vhost do |vhost|
        vhost.queues["test1"] = LavinMQ::Queue.new(vhost, "test1", arguments: LavinMQ::AMQP::Table.new({"x-max-length" => 1_i64}))
        vhost.queues["test2"] = LavinMQ::Queue.new(vhost, "test2", arguments: LavinMQ::AMQP::Table.new({"x-max-length" => 11_i64}))
        vhost.add_policy("test", ".*", "all", definitions, 100_i8)
        sleep 0.01
        vhost.queues["test1"].@max_length.should eq 1
        vhost.queues["test2"].@max_length.should eq 10
        vhost.delete_policy("test")
        sleep 0.01
        vhost.queues["test1"].@max_length.should eq 1
        vhost.queues["test2"].@max_length.should eq 11
      end
    end
  end
end
