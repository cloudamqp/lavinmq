require "../spec_helper"
require "../../src/lavinmq/mqtt/permission_group"

private def group(members = Array(String).new, rules = Array(LavinMQ::MQTT::PermissionGroup::Rule).new)
  LavinMQ::MQTT::PermissionGroup.new("g", "/", members, rules)
end

private def rule(identifier, pattern = "a/#", read = false, write = false)
  LavinMQ::MQTT::PermissionGroup::Rule.new(identifier, pattern, read: read, write: write)
end

describe LavinMQ::MQTT::PermissionGroup do
  describe "#add_member" do
    it "adds a new member and reports it" do
      g = group
      g.add_member("c1").should be_true
      g.members.should eq ["c1"]
    end

    it "reports an existing member without duplicating it" do
      g = group(["c1"])
      g.add_member("c1").should be_false
      g.members.should eq ["c1"]
    end
  end

  describe "#remove_member" do
    it "removes a member and reports it" do
      g = group(["c1", "c2"])
      g.remove_member("c1").should be_true
      g.members.should eq ["c2"]
    end

    it "reports a missing member" do
      group.remove_member("c1").should be_false
    end
  end

  describe "#put_rule" do
    it "adds a new rule and returns nil" do
      g = group
      g.put_rule(rule("r", "a/#", read: true)).should be_nil
      g.rules["r"].pattern.should eq "a/#"
    end

    it "replaces a rule with the same identifier and returns the old rule" do
      g = group(rules: [rule("r", "a/#", read: true)])
      old = g.put_rule(rule("r", "b/#", write: true))
      old.should_not be_nil
      old.not_nil!.pattern.should eq "a/#"
      g.rules.size.should eq 1
      g.rules["r"].pattern.should eq "b/#"
    end

    it "rejects an invalid topic filter and leaves the group unchanged" do
      g = group(rules: [rule("r", "a/#", read: true)])
      expect_raises(ArgumentError, /Invalid MQTT topic filter/) do
        g.put_rule(rule("r", "a/#/b", read: true))
      end
      g.rules["r"].pattern.should eq "a/#"
    end

    it "rejects an invalid identifier" do
      expect_raises(ArgumentError, /Invalid rule identifier/) do
        group.put_rule(rule("bad id", "a/#", read: true))
      end
    end
  end

  describe "#delete_rule" do
    it "removes a rule and returns it" do
      g = group(rules: [rule("r", "a/#", read: true)])
      g.delete_rule("r").not_nil!.pattern.should eq "a/#"
      g.rules.should be_empty
    end

    it "returns nil for an unknown identifier" do
      group.delete_rule("r").should be_nil
    end
  end

  describe "rules" do
    it "is keyed on the rule identifier" do
      g = group(rules: [rule("r1", "a/#"), rule("r2", "b/#")])
      g.rules.keys.should eq ["r1", "r2"]
    end

    it "refuses duplicate identifiers at construction" do
      expect_raises(ArgumentError, /Duplicate rule identifier/) do
        group(rules: [rule("r", "a/#"), rule("r", "b/#")])
      end
    end
  end

  describe "JSON" do
    it "writes rules as an array and reads it back" do
      g = group(["c1"], [rule("r", "a/#", read: true)])
      json = g.to_json
      JSON.parse(json)["rules"].as_a.first["identifier"].should eq "r"
      reloaded = LavinMQ::MQTT::PermissionGroup.from_json(json)
      reloaded.rules["r"].pattern.should eq "a/#"
      reloaded.members.should eq ["c1"]
    end

    it "refuses duplicate identifiers when loading" do
      rules = %([{"identifier":"r","pattern":"a/#","read":true},{"identifier":"r","pattern":"b/#","read":true}])
      json = %({"name":"g","vhost":"/","members":[],"rules":#{rules}})
      expect_raises(ArgumentError, /Duplicate rule identifier/) do
        LavinMQ::MQTT::PermissionGroup.from_json(json)
      end
    end
  end
end
