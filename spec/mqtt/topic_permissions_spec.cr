require "../spec_helper"
require "../../src/lavinmq/mqtt/topic_permissions"

describe LavinMQ::MQTT::TopicPermissions do
  it "unions rules across groups and substitutes variables" do
    g1 = LavinMQ::Auth::PermissionGroup.new("a", "mqtt", false, ["alice"], [
      LavinMQ::Auth::PermissionGroup::Rule.new("chat/{client_id}/#", read: true, write: true),
      LavinMQ::Auth::PermissionGroup::Rule.new("u/{username}/#", read: true, write: true),
    ])
    g2 = LavinMQ::Auth::PermissionGroup.new("all", "mqtt", true, [] of String, [
      LavinMQ::Auth::PermissionGroup::Rule.new("broadcast/#", read: true, write: false),
    ])

    tp = LavinMQ::MQTT::TopicPermissions.build([g1, g2], "alice", "dev99")

    tp.write.matches?("chat/dev99/room1").should be_true  # client_id expanded
    tp.write.matches?("chat/alice/room1").should be_false # client_id is not the username
    tp.write.matches?("u/alice/x").should be_true         # username expanded
    tp.write.matches?("broadcast/news").should be_false   # broadcast is read-only
    tp.read.matches?("broadcast/news").should be_true
    tp.read.matches?("chat/bob/room1").should be_false
  end
end
