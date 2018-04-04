require "./spec_helper"

describe AvalancheMQ::VHost do
  log = Logger.new(File.open("/dev/null", "w"))
  vhost = AvalancheMQ::VHost.new("add_policy", "/tmp/spec", log)
  definitions = {
      "max-length" => 10,
      "alternate-exchange" => "dead-letters" } of String => AvalancheMQ::Policy::Value

  it "should be able to add policy" do
    vhost.add_policy("test", "^.*$", "all", definitions, -10_i8)
    vhost.policies.size.should eq 1
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
  end

  it "should validate pattern" do
    expect_raises(ArgumentError) do
      vhost.add_policy("test", "(bad", "all", definitions, -10_i8)
    end
  end

  it "should validate appy_to" do
    expect_raises(ArgumentError) do
      vhost.add_policy("test", "^.*$", "bad", definitions, -10_i8)
    end
  end
end
