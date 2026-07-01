require "../spec_helper"
require "../../src/lavinmq/mqtt/topic_permissions"

describe LavinMQ::MQTT::TopicPermissions do
  it "unions rules across groups and substitutes the username" do
    g1 = LavinMQ::Auth::PermissionGroup.new("a", "mqtt", false, ["alice"], [
      LavinMQ::Auth::PermissionGroup::Rule.new("u/{username}/#", read: true, write: true),
    ])
    g2 = LavinMQ::Auth::PermissionGroup.new("all", "mqtt", true, [] of String, [
      LavinMQ::Auth::PermissionGroup::Rule.new("broadcast/#", read: true, write: false),
    ])

    tp = LavinMQ::MQTT::TopicPermissions.build([g1, g2], "alice")

    tp.write.matches?("u/alice/x").should be_true       # username expanded
    tp.write.matches?("u/bob/x").should be_false        # another user's namespace denied
    tp.write.matches?("broadcast/news").should be_false # broadcast is read-only
    tp.read.matches?("broadcast/news").should be_true
    tp.read.matches?("u/bob/x").should be_false
  end

  it "drops {username} rules when the username has a wildcard or separator, instead of over-granting" do
    # A username of "+" would expand "data/{username}/#" to "data/+/#",
    # matching every user's subtree. Fail closed: drop the rule.
    g = LavinMQ::Auth::PermissionGroup.new("g", "mqtt", false, ["+"], [
      LavinMQ::Auth::PermissionGroup::Rule.new("data/{username}/#", read: true, write: true),
    ])

    tp = LavinMQ::MQTT::TopicPermissions.build([g], "+")

    tp.read.matches?("data/bob/x").should be_false
    tp.write.matches?("data/bob/x").should be_false
  end

  it "still applies static rules for a username that can't be substituted" do
    g = LavinMQ::Auth::PermissionGroup.new("g", "mqtt", false, ["a/b"], [
      LavinMQ::Auth::PermissionGroup::Rule.new("data/{username}/#", read: true, write: false),
      LavinMQ::Auth::PermissionGroup::Rule.new("public/#", read: true, write: false),
    ])

    tp = LavinMQ::MQTT::TopicPermissions.build([g], "a/b")

    tp.read.matches?("public/news").should be_true # static rule unaffected
  end
end
