require "./spec_helper"

describe AvalancheMQ::VHost do
  log = Logger.new(STDOUT)
  # log.level = Logger::DEBUG
  vhost = AvalancheMQ::VHost.new("add_policy", "/tmp/spec", log)
  definitions = {
      "max-length" => 10,
      "alternate-exchange" => "dead-letters" } of String => AvalancheMQ::Policy::Value

  it "should be able to add policy" do
    vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
    vhost.policies.size.should eq 1
    vhost.remove_policy("test")
  end

  it "should be able to list policies" do
    vhost = AvalancheMQ::VHost.new("add_remove_policy", "/tmp/spec", log)
    vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
    vhost.remove_policy("test")
    vhost.policies.size.should eq 0
  end

  it "should overwrite policy with same name" do
    vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
    vhost.add_policy("test", "^.*$", "exchanges", definitions, 10_i8)
    vhost.policies.size.should eq 1
    vhost.policies["test"].apply_to.should eq "exchanges"
    vhost.remove_policy("test")
  end

  it "should validate pattern" do
    expect_raises(ArgumentError) do
      vhost.add_policy("test", "(bad", "all", definitions, -10_i8)
    end
    vhost.remove_policy("test")
  end

  it "should validate appy_to" do
    expect_raises(ArgumentError) do
      vhost.add_policy("test", "^.*$", "bad", definitions, -10_i8)
    end
    vhost.remove_policy("test")
  end

  it "should apply policy" do
    definitions = { "max-length" => 1 } of String => AvalancheMQ::Policy::Value
    vhost.queues["test"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml", "^.*$", "queues", definitions, 11_i8)
    Fiber.yield
    vhost.queues["test"].policy.not_nil!.name.should eq "ml"
    vhost.remove_policy("ml")
  end

  it "should respect priroty" do
    definitions = { "max-length" => 1 } of String => AvalancheMQ::Policy::Value
    vhost.queues["test2"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml2", "^.*$", "queues", definitions, 1_i8)
    vhost.add_policy("ml1", "^.*$", "queues", definitions, 0_i8)
    Fiber.yield
    vhost.queues["test2"].policy.not_nil!.name.should eq "ml2"
  end
end
