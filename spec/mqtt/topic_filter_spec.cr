require "../spec_helper"
require "../../src/lavinmq/mqtt/topic_filter"

describe LavinMQ::MQTT::TopicFilter do
  describe ".valid_filter?" do
    it "accepts well-formed MQTT topic filters" do
      LavinMQ::MQTT::TopicFilter.valid_filter?("chat/alice/room1").should be_true
      LavinMQ::MQTT::TopicFilter.valid_filter?("chat/{client_id}/#").should be_true
      LavinMQ::MQTT::TopicFilter.valid_filter?("a/+/c").should be_true
      LavinMQ::MQTT::TopicFilter.valid_filter?("#").should be_true
      LavinMQ::MQTT::TopicFilter.valid_filter?("+").should be_true
      LavinMQ::MQTT::TopicFilter.valid_filter?("+/tennis/#").should be_true
      LavinMQ::MQTT::TopicFilter.valid_filter?("a//b").should be_true # empty levels are legal
    end

    it "rejects a '#' that is not the last and sole token of its level" do
      LavinMQ::MQTT::TopicFilter.valid_filter?("secret/#/temp").should be_false
      LavinMQ::MQTT::TopicFilter.valid_filter?("a#").should be_false
      LavinMQ::MQTT::TopicFilter.valid_filter?("sport/tennis#").should be_false
      LavinMQ::MQTT::TopicFilter.valid_filter?("#/a").should be_false
    end

    it "rejects a '+' that is not the sole token of its level" do
      LavinMQ::MQTT::TopicFilter.valid_filter?("a/b+").should be_false
      LavinMQ::MQTT::TopicFilter.valid_filter?("+a").should be_false
      LavinMQ::MQTT::TopicFilter.valid_filter?("sport+/tennis").should be_false
    end

    it "rejects an empty filter" do
      LavinMQ::MQTT::TopicFilter.valid_filter?("").should be_false
    end

    it "rejects a filter with too many levels" do
      LavinMQ::MQTT::TopicFilter.valid_filter?(Array.new(201, "a").join('/')).should be_true  # 200 separators
      LavinMQ::MQTT::TopicFilter.valid_filter?(Array.new(202, "a").join('/')).should be_false # 201 separators
    end
  end
end
