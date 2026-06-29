require "./spec_helper"
require "../../src/lavinmq/mqtt/topic_translator"

describe LavinMQ::MQTT::TopicTranslator do
  describe ".mqtt_to_amqp" do
    # { mqtt topic, expected amqp routing key }
    [
      {"a", "a"},
      {"a/b/c", "a.b.c"},
      {"sensors/device1/temp", "sensors.device1.temp"},
      {"a//c", "a..c"},           # empty level preserved
      {"/a", ".a"},               # leading separator preserved
      {"a/", "a."},               # trailing separator preserved
      {"v1.2/data", "v1.2.data"}, # documented caveat: literal '.' collapses into a level
    ].each do |(topic, expected)|
      it "translates #{topic.inspect} to #{expected.inspect}" do
        LavinMQ::MQTT::TopicTranslator.mqtt_to_amqp(topic).should eq expected
      end
    end

    it "is byte-length preserving" do
      topic = "a/bb/ccc/dddd"
      LavinMQ::MQTT::TopicTranslator.mqtt_to_amqp(topic).bytesize.should eq topic.bytesize
    end
  end
end
