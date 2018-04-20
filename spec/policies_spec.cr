require "./spec_helper"

describe AvalancheMQ::VHost do
  log = Logger.new(STDOUT)
  log.level = Logger::ERROR
  FileUtils.rm_rf("/tmp/spec")
  vhost = AvalancheMQ::VHost.new("add_policy", "/tmp/spec", log)
  definitions = JSON::Any.new({
    "max-length" => 10_i64,
    "alternate-exchange" => "dead-letters" } of String => JSON::Type)

  it "should be able to add policy" do
    vhost.add_policy("test", /^.*$/, AvalancheMQ::Policy::Target::All, definitions, -10_i8)
    vhost.policies.size.should eq 1
    vhost.delete_policy("test")
  end

  it "should be able to list policies" do
    vhost2 = AvalancheMQ::VHost.new("add_remove_policy", "/tmp/spec_lp", log)
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
    definitions = JSON::Any.new({ "max-length" => 1_i64 } of String => JSON::Type)
    vhost.queues["test"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 11_i8)
    Fiber.yield
    vhost.queues["test"].policy.not_nil!.name.should eq "ml"
    vhost.delete_policy("ml")
  end

  it "should respect priroty" do
    definitions = JSON::Any.new({ "max-length" => 1_i64 } of String => JSON::Type)
    vhost.queues["test2"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml2", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 1_i8)
    vhost.add_policy("ml1", /^.*$/, AvalancheMQ::Policy::Target::Queues, definitions, 0_i8)
    Fiber.yield
    vhost.queues["test2"].policy.not_nil!.name.should eq "ml2"
  end
end
