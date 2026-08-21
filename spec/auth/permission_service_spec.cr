require "../spec_helper"
require "../../src/lavinmq/auth/permission_service"

private def group(name, members, rules, protocol = "mqtt")
  LavinMQ::Auth::PermissionGroup.new(name, protocol, members, rules)
end

private def rule(pattern, read = false, write = false)
  LavinMQ::Auth::PermissionGroup::Rule.new(pattern, read: read, write: write)
end

private def with_service(&)
  data_dir = File.tempname
  Dir.mkdir_p data_dir
  begin
    yield LavinMQ::Auth::PermissionService.new(data_dir, nil)
  ensure
    FileUtils.rm_rf data_dir
  end
end

describe LavinMQ::Auth::PermissionService do
  it "allows everything with no groups" do
    with_service do |service|
      service.mqtt_in_use?.should be_false
      service.can_write?("c1", "a/b").should be_true
    end
  end

  it "rejects an invalid group on put" do
    with_service do |service|
      expect_raises(ArgumentError, /Invalid MQTT topic filter/) do
        service.put(group("g", ["*"], [rule("a/#/b", write: true)]))
      end
      expect_raises(ArgumentError, /Unsupported protocol/) do
        service.put(group("g", ["*"], [rule("a/#", write: true)], protocol: "amqp"))
      end
      service.mqtt_in_use?.should be_false
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
      service.mqtt_in_use?.should be_false
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
      first = LavinMQ::Auth::PermissionService.new(data_dir, nil)
      first.put(group("g", ["c1"], [rule("a/#", write: true)]))

      second = LavinMQ::Auth::PermissionService.new(data_dir, nil)
      second.mqtt_in_use?.should be_true
      second.can_write?("c1", "a/b").should be_true
    ensure
      FileUtils.rm_rf data_dir
    end
  end

  it "tolerates invalid or non-mqtt groups loaded from disk" do
    data_dir = File.tempname
    Dir.mkdir_p data_dir
    begin
      File.write File.join(data_dir, "permission_groups.json"), <<-JSON
        [
          {"name":"amqp_group","protocol":"amqp","members":["*"],"rules":[{"pattern":"a/#","read":true,"write":true}]},
          {"name":"g","protocol":"mqtt","members":["c1"],"rules":[{"pattern":"bad/#/x","write":true},{"pattern":"ok/#","write":true}]}
        ]
        JSON
      service = LavinMQ::Auth::PermissionService.new(data_dir, nil)
      service.mqtt_in_use?.should be_true
      service.can_write?("c1", "ok/x").should be_true
      service.can_write?("c1", "bad/y/x").should be_false
      service.can_write?("c2", "a/b").should be_false
    ensure
      FileUtils.rm_rf data_dir
    end
  end

  it "leaves mqtt_in_use? false for a group with rules but no members" do
    with_service do |service|
      service.put(group("g", Array(String).new, [rule("a/#", read: true)]))
      service.mqtt_in_use?.should be_false
      service.can_read?("c1", "a/b").should be_true
    end
  end

  it "leaves mqtt_in_use? false for a group with an empty rules array" do
    with_service do |service|
      service.put(group("g", ["*"], [] of LavinMQ::Auth::PermissionGroup::Rule))
      service.mqtt_in_use?.should be_false
    end
  end

  it "flips mqtt_in_use? true when an empty-rules group gains a valid rule" do
    with_service do |service|
      service.put(group("g", ["*"], [] of LavinMQ::Auth::PermissionGroup::Rule))
      service.mqtt_in_use?.should be_false
      service.put(group("g", ["*"], [rule("a/#", read: true)]))
      service.mqtt_in_use?.should be_true
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
