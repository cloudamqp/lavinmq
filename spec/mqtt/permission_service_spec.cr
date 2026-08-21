require "../spec_helper"
require "../../src/lavinmq/mqtt/permission_service"

private def group(name, members, rules)
  LavinMQ::MQTT::PermissionGroup.new(name, "/", members, rules)
end

private def rule(pattern, read = false, write = false)
  identifier = pattern.gsub(/[^A-Za-z0-9-]/, "-")
  LavinMQ::MQTT::PermissionGroup::Rule.new(identifier, pattern, read: read, write: write)
end

private def with_service(&)
  data_dir = File.tempname
  Dir.mkdir_p data_dir
  begin
    yield LavinMQ::MQTT::PermissionService.new(data_dir, nil)
  ensure
    FileUtils.rm_rf data_dir
  end
end

describe LavinMQ::MQTT::PermissionService do
  it "allows everything with no groups" do
    with_service do |service|
      service.in_use?.should be_false
      service.can_write?("c1", "a/b").should be_true
    end
  end

  it "rejects an invalid group on put" do
    with_service do |service|
      expect_raises(ArgumentError, /Invalid MQTT topic filter/) do
        service.put(group("g", ["*"], [rule("a/#/b", write: true)]))
      end
      service.in_use?.should be_false
    end
  end

  it "grants only the requested verb" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      service.can_read?("c1", "a/b").should be_true
      service.can_write?("c1", "a/b").should be_false
    end
  end

  it "applies a group only to its members" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", write: true)]))
      service.can_write?("c1", "a/b").should be_true
      service.can_write?("c2", "a/b").should be_false
    end
  end

  it "never attributes one group's rules to another group's members" do
    with_service do |service|
      service.put(group("a", ["c1"], [rule("a/#", read: true)]))
      service.put(group("b", ["c2"], [rule("b/#", write: true)]))

      service.can_read?("c1", "a/x").should be_true
      service.can_read?("c2", "a/x").should be_false
      service.can_write?("c1", "b/x").should be_false
      service.can_write?("c2", "b/x").should be_true

      service.can_write?("c1", "a/x").should be_false
      service.can_read?("c2", "b/x").should be_false
    end
  end

  it "applies a wildcard-member group to every client" do
    with_service do |service|
      service.put(group("g", ["*"], [rule("a/#", write: true)]))
      service.can_write?("anything", "a/b").should be_true
    end
  end

  it "binds {client_id} to the requesting client" do
    with_service do |service|
      service.put(group("g", ["*"], [rule("data/{client_id}/#", read: true, write: true)]))
      service.can_write?("c1", "data/c1/temp").should be_true
      service.can_write?("c1", "data/c2/temp").should be_false
    end
  end

  it "reflects an update on the next check" do
    with_service do |service|
      service.put(group("g", ["*"], [rule("a/#", write: true)]))
      service.can_write?("c1", "a/b").should be_true

      service.put(group("g", ["*"], [rule("a/#", read: true)]))
      service.can_write?("c1", "a/b").should be_false
      service.can_read?("c1", "a/b").should be_true
    end
  end

  it "reflects a delete on the next check" do
    with_service do |service|
      service.put(group("g", ["c1"], [rule("a/#", read: true)]))
      service.can_read?("c2", "a/b").should be_false
      service.delete("g")
      service.can_read?("c2", "a/b").should be_true
      service.in_use?.should be_false
    end
  end

  it "fails closed on {client_id} matching when the client id is the '#' wildcard" do
    with_service do |service|
      service.put(group("g", ["*"], [
        rule("data/{client_id}/#", read: true),
        rule("static/topic", read: true),
      ]))
      service.can_read?("#", "data/anything/temp").should be_false
      service.can_read?("#", "static/topic").should be_true
    end
  end

  it "survives a reload from disk" do
    data_dir = File.tempname
    Dir.mkdir_p data_dir
    begin
      first = LavinMQ::MQTT::PermissionService.new(data_dir, nil)
      first.put(group("g", ["c1"], [rule("a/#", write: true)]))

      second = LavinMQ::MQTT::PermissionService.new(data_dir, nil)
      second.in_use?.should be_true
      second.can_write?("c1", "a/b").should be_true
    ensure
      FileUtils.rm_rf data_dir
    end
  end

  it "tolerates invalid patterns loaded from disk" do
    data_dir = File.tempname
    Dir.mkdir_p data_dir
    begin
      File.write File.join(data_dir, "mqtt_permissions.json"), <<-JSON
        [
          {"name":"g","vhost":"/","members":["c1"],"rules":[{"identifier":"bad","pattern":"bad/#/x","write":true},{"identifier":"ok","pattern":"ok/#","write":true}]}
        ]
        JSON
      service = LavinMQ::MQTT::PermissionService.new(data_dir, nil)
      service.in_use?.should be_true
      service.can_write?("c1", "ok/x").should be_true
      service.can_write?("c1", "bad/y/x").should be_false
    ensure
      FileUtils.rm_rf data_dir
    end
  end

  it "leaves in_use? false for a group with rules but no members" do
    with_service do |service|
      service.put(group("g", Array(String).new, [rule("a/#", read: true)]))
      service.in_use?.should be_false
      service.can_read?("c1", "a/b").should be_true
    end
  end

  it "leaves in_use? false for a group with an empty rules array" do
    with_service do |service|
      service.put(group("g", ["*"], [] of LavinMQ::MQTT::PermissionGroup::Rule))
      service.in_use?.should be_false
    end
  end

  it "flips in_use? true when an empty-rules group gains a valid rule" do
    with_service do |service|
      service.put(group("g", ["*"], [] of LavinMQ::MQTT::PermissionGroup::Rule))
      service.in_use?.should be_false
      service.put(group("g", ["*"], [rule("a/#", read: true)]))
      service.in_use?.should be_true
    end
  end

  it "gives a client the union of rules from every group it is a member of" do
    with_service do |service|
      service.put(group("a", ["c1"], [rule("a/#", read: true)]))
      service.put(group("b", ["c1"], [rule("b/#", write: true)]))
      service.can_read?("c1", "a/x").should be_true
      service.can_write?("c1", "b/x").should be_true
      service.can_write?("c1", "a/x").should be_false
      service.can_read?("c1", "b/x").should be_false
    end
  end
end
