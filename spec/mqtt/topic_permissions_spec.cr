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
end
