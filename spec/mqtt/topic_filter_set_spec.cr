require "../spec_helper"
require "../../src/lavinmq/mqtt/topic_filter_set"

describe LavinMQ::MQTT::TopicFilterSet do
  describe ".expand" do
    it "substitutes username and client_id" do
      LavinMQ::MQTT::TopicFilterSet
        .expand("chat/{client_id}/#", "alice", "dev42")
        .should eq "chat/dev42/#"
      LavinMQ::MQTT::TopicFilterSet
        .expand("u/{username}/+", "alice", "dev42")
        .should eq "u/alice/+"
    end
  end

  describe ".filters_overlap?" do
    it "is true when two filters share a concrete topic" do
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("chat/alice/#", "chat/alice/room1").should be_true
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("chat/alice/#", "chat/alice").should be_true
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("+/temp", "room/temp").should be_true
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("#", "anything/at/all").should be_true
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("a/b", "a/#").should be_true
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("+", "+").should be_true
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("a/+/c", "a/b/c").should be_true
    end

    it "is false when filters cannot share any topic" do
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("chat/alice/#", "chat/bob/#").should be_false
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("a/+", "a/b/c").should be_false
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("a/+/c", "a/b/d").should be_false
      LavinMQ::MQTT::TopicFilterSet.filters_overlap?("a", "a/b").should be_false
    end
  end

  describe "#matches? and #overlaps?" do
    it "matches concrete topics and detects overlap with subscription filters" do
      set = LavinMQ::MQTT::TopicFilterSet.new
      set.add("chat/alice/#")

      set.matches?("chat/alice/room1").should be_true
      set.matches?("chat/bob/room1").should be_false

      set.overlaps?("chat/alice/room1").should be_true # exact subscription
      set.overlaps?("#").should be_true                # broad sub overlaps allowed prefix
      set.overlaps?("chat/bob/#").should be_false      # zero overlap
    end
  end
end
