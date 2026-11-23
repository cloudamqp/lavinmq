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

  describe ".qos" do
    it "reads the QoS back from the arguments of qos_arguments" do
      LavinMQ::MQTT.qos(LavinMQ::MQTT.qos_arguments(0u8)).should eq 0u8
      LavinMQ::MQTT.qos(LavinMQ::MQTT.qos_arguments(1u8)).should eq 1u8
      LavinMQ::MQTT.qos(LavinMQ::MQTT.qos_arguments(2u8)).should eq 1u8
    end

    it "grants QoS 2 as QoS 1" do
      arguments = LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => 2u8})
      LavinMQ::MQTT.qos(arguments).should eq 1u8
    end

    it "accepts any integer type, not just UInt8" do
      LavinMQ::MQTT.qos(LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => 1})).should eq 1u8
      LavinMQ::MQTT.qos(LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => 2i64})).should eq 1u8
    end

    it "is QoS 0 without arguments" do
      LavinMQ::MQTT.qos(nil).should eq 0u8
      LavinMQ::MQTT.qos(LavinMQ::AMQP::Table.new).should eq 0u8
    end

    it "is QoS 0 for a non-integer value" do
      LavinMQ::MQTT.qos(LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => "1"})).should eq 0u8
    end

    it "is QoS 0 for a negative value" do
      LavinMQ::MQTT.qos(LavinMQ::AMQP::Table.new({LavinMQ::MQTT::QOS_HEADER => -1})).should eq 0u8
    end
  end
end
