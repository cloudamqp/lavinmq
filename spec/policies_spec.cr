require "./spec_helper"

describe AvalancheMQ::VHost do
  log = Logger.new(STDOUT)
  log.level = Logger::ERROR
  FileUtils.rm_rf("/tmp/spec")
  vhost = AvalancheMQ::VHost.new("add_policy", "/tmp/spec", log)
  definitions = {
    "max-length" => 10_i64,
    "alternate-exchange" => "dead-letters" } of String => AvalancheMQ::ParameterValue

  it "should be able to add policy" do
    vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
    vhost.policies.size.should eq 1
    vhost.delete_policy("test")
  end

  it "should be able to list policies" do
    vhost2 = AvalancheMQ::VHost.new("add_remove_policy", "/tmp/spec_lp", log)
    vhost2.add_policy("test", " ^.*$", "all", definitions, -10_i8)
    vhost2.delete_policy("test")
    vhost2.policies.size.should eq 0
    vhost2.delete
  end

  it "should overwrite policy with same name" do
    vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
    vhost.add_policy("test", "^.*$", "exchanges", definitions, 10_i8)
    vhost.policies.size.should eq 1
    vhost.policies["test"].apply_to.should eq "exchanges"
    vhost.delete_policy("test")
  end

  it "should validate pattern" do
    expect_raises(ArgumentError) do
      vhost.add_policy("test", "(bad", "all", definitions, -10_i8)
    end
    vhost.delete_policy("test")
  end

  it "should validate appy_to" do
    expect_raises(ArgumentError) do
      vhost.add_policy("test", "^.*$", "bad", definitions, -10_i8)
    end
    vhost.delete_policy("test")
  end

  it "should apply policy" do
    definitions = { "max-length" => 1_i64 } of String => AvalancheMQ::ParameterValue
    vhost.queues["test"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml", "^.*$", "queues", definitions, 11_i8)
    Fiber.yield
    vhost.queues["test"].policy.not_nil!.name.should eq "ml"
    vhost.delete_policy("ml")
  end

  it "should respect priroty" do
    definitions = { "max-length" => 1_i64 } of String => AvalancheMQ::ParameterValue
    vhost.queues["test2"] = AvalancheMQ::Queue.new(vhost, "test")
    vhost.add_policy("ml2", "^.*$", "queues", definitions, 1_i8)
    vhost.add_policy("ml1", "^.*$", "queues", definitions, 0_i8)
    Fiber.yield
    vhost.queues["test2"].policy.not_nil!.name.should eq "ml2"
  end
end
