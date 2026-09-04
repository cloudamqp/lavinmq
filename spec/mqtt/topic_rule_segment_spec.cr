require "../spec_helper"
require "../../src/lavinmq/mqtt/topic_rule_segment"

alias TS = LavinMQ::MQTT::TopicRuleSegment

private def matches?(pattern : String, topic : String, client_id = "c1") : Bool
  chain = TS.compile(pattern)
  raise "pattern #{pattern.inspect} did not compile" unless chain
  TS.matches?(chain, topic, client_id)
end

describe LavinMQ::MQTT::TopicRuleSegment do
  it "matches a literal filter exactly" do
    matches?("a/b", "a/b").should be_true
    matches?("a/b", "a/c").should be_false
    matches?("a/b", "a").should be_false
    matches?("a/b", "a/b/c").should be_false
  end

  it "matches a single level with +" do
    matches?("a/+/c", "a/b/c").should be_true
    matches?("a/+/c", "a/x/c").should be_true
    matches?("a/+/c", "a/c").should be_false
    matches?("a/+/c", "a/b/x/c").should be_false
  end

  it "matches the remainder with #, including zero levels" do
    matches?("a/#", "a").should be_true
    matches?("a/#", "a/b").should be_true
    matches?("a/#", "a/b/c").should be_true
    matches?("a/#", "b").should be_false
  end

  it "matches everything with a bare #" do
    matches?("#", "a").should be_true
    matches?("#", "a/b").should be_true
  end

  it "matches the client id level only for that client" do
    matches?("data/{client_id}/#", "data/c1/temp", client_id: "c1").should be_true
    matches?("data/{client_id}/#", "data/c2/temp", client_id: "c1").should be_false
  end

  it "does not treat a client id containing a separator as a wildcard" do
    matches?("data/{client_id}", "data/a/b", client_id: "a/b").should be_false
  end

  it "does not compile an invalid filter" do
    TS.compile("a/#/b").should be_nil
  end
end
