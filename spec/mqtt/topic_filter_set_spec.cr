require "../spec_helper"
require "../../src/lavinmq/mqtt/topic_filter_set"

describe LavinMQ::MQTT::TopicFilterSet do
  describe ".expand" do
    it "substitutes the username" do
      LavinMQ::MQTT::TopicFilterSet
        .expand("u/{username}/+", "alice")
        .should eq "u/alice/+"
    end
  end

  describe ".valid_filter?" do
    it "accepts well-formed MQTT topic filters" do
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("chat/alice/room1").should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("chat/{username}/#").should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("a/+/c").should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("#").should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("+").should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("+/tennis/#").should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("a//b").should be_true # empty levels are legal
    end

    it "rejects a '#' that is not the last and sole token of its level" do
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("secret/#/temp").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("a#").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("sport/tennis#").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("#/a").should be_false
    end

    it "rejects a '+' that is not the sole token of its level" do
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("a/b+").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("+a").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("sport+/tennis").should be_false
    end

    it "rejects an empty filter" do
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("").should be_false
    end

    it "rejects a filter longer than the MQTT wire maximum (65535 bytes)" do
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("a" * 65_535).should be_true
      LavinMQ::MQTT::TopicFilterSet.valid_filter?("a" * 65_536).should be_false
    end

    it "rejects a filter with too many levels" do
      LavinMQ::MQTT::TopicFilterSet.valid_filter?(Array.new(201, "a").join('/')).should be_true  # 200 separators
      LavinMQ::MQTT::TopicFilterSet.valid_filter?(Array.new(202, "a").join('/')).should be_false # 201 separators
    end
  end

  describe ".valid_substitution?" do
    it "accepts a plain single-level value" do
      LavinMQ::MQTT::TopicFilterSet.valid_substitution?("alice").should be_true
    end

    it "rejects empty values or values with a separator or wildcard" do
      LavinMQ::MQTT::TopicFilterSet.valid_substitution?("").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_substitution?("a/b").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_substitution?("+").should be_false
      LavinMQ::MQTT::TopicFilterSet.valid_substitution?("#").should be_false
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
