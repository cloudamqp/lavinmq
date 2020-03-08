require "./spec_helper"

describe AvalancheMQ::VHost do
  log = Logger.new(STDOUT)
  log.level = LOG_LEVEL

  vhost = AvalancheMQ::VHost.new("add_policy", "/tmp/spec", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
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
    vhost.queues["test"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    sleep 0.01
    vhost.queues["test"].policy.try(&.name).should eq "test"
    vhost.delete_policy("test")
    sleep 0.01
    vhost.queues["test"].policy.should be_nil
  ensure
    vhost.delete_queue("test")
  end

  it "should be able to list policies" do
    vhost2 = AvalancheMQ::VHost.new("add_remove_policy", "/tmp/spec_lp", log, AvalancheMQ::User.create("", "", "MD5", [] of AvalancheMQ::Tag))
    vhost2.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    vhost2.delete_policy("test")
    vhost2.policies.size.should eq 0
    vhost2.delete
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
    vhost.delete_queue("test")
  end

  it "should respect priroty" do
    definitions = {"max-length" => JSON::Any.new(1_i64)} of String => JSON::Any
    vhost.queues["test2"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml2", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 1_i8)
    vhost.add_policy("ml1", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 0_i8)
    sleep 0.01
    vhost.queues["test2"].policy.not_nil!.name.should eq "ml2"
  ensure
    vhost.delete_queue("test2")
    vhost.delete_policy("ml2")
    vhost.delete_policy("ml1")
  end

  it "should remove effect of deleted policy" do
    definitions = {"max-length" => JSON::Any.new(10_i64)} of String => JSON::Any
    s.vhosts["/"].add_policy("mld", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, 12_i8)
    with_channel do |ch|
      ch.confirm_select
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
    vhost.delete_queue("mld")
  end
end
