require "./spec_helper"

describe LavinMQ::MQTT do
  describe ".qos_arguments" do
    it "returns the QoS 0 constant for QoS 0" do
      LavinMQ::MQTT.qos_arguments(0u8).should be(LavinMQ::MQTT::QOS0_ARGUMENTS)
    end

    it "returns the QoS 1 constant for QoS 1" do
      LavinMQ::MQTT.qos_arguments(1u8).should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end

    it "returns the QoS 1 constant for QoS 2, which is granted as QoS 1" do
      LavinMQ::MQTT.qos_arguments(2u8).should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end

    it "carries the QoS under the QoS header" do
      LavinMQ::MQTT.qos_arguments(0u8)[LavinMQ::MQTT::QOS_HEADER].should eq 0u8
      LavinMQ::MQTT.qos_arguments(1u8)[LavinMQ::MQTT::QOS_HEADER].should eq 1u8
    end
  end
end
