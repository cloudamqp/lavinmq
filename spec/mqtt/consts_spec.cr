require "./spec_helper"

private def opts(qos, no_local = false, retain_as_published = false)
  LavinMQ::MQTT::SubscriptionOptions.new(qos.to_u8, no_local, retain_as_published)
end

private def table(hash)
  LavinMQ::AMQP::Table.new(hash)
end

describe LavinMQ::MQTT do
  describe ".granted_qos" do
    it "passes through the QoS levels it can deliver" do
      LavinMQ::MQTT.granted_qos(0u8).should eq 0u8
      LavinMQ::MQTT.granted_qos(1u8).should eq 1u8
    end

    it "grants QoS 2 as QoS 1 [MQTT-3.9.3-1]" do
      LavinMQ::MQTT.granted_qos(2u8).should eq 1u8
    end

    it "accepts any integer type, since a binding argument is not parsed by MQTT" do
      LavinMQ::MQTT.granted_qos(1).should eq 1u8
      LavinMQ::MQTT.granted_qos(2i64).should eq 1u8
      LavinMQ::MQTT.granted_qos(Int64::MAX).should eq 1u8
    end

    it "is QoS 0 for a missing or negative value" do
      LavinMQ::MQTT.granted_qos(nil).should eq 0u8
      LavinMQ::MQTT.granted_qos(-1).should eq 0u8
      LavinMQ::MQTT.granted_qos(Int64::MIN).should eq 0u8
    end
  end

  describe ".session_name" do
    it "prefixes the client id" do
      LavinMQ::MQTT.session_name("sub").should eq "mqtt.sub"
    end
  end

  describe ".subscription_arguments" do
    it "returns the QoS 0 constant for QoS 0" do
      LavinMQ::MQTT.subscription_arguments(opts(0)).should be(LavinMQ::MQTT::QOS0_ARGUMENTS)
    end

    it "returns the QoS 1 constant for QoS 1" do
      LavinMQ::MQTT.subscription_arguments(opts(1)).should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end

    it "returns the QoS 1 constant for QoS 2, which is granted as QoS 1" do
      LavinMQ::MQTT.subscription_arguments(opts(2)).should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end

    it "carries the QoS under the QoS header" do
      LavinMQ::MQTT.subscription_arguments(opts(0))[LavinMQ::MQTT::QOS_HEADER].should eq 0u8
      LavinMQ::MQTT.subscription_arguments(opts(1))[LavinMQ::MQTT::QOS_HEADER].should eq 1u8
    end

    it "omits the option keys entirely when both are false" do
      arguments = LavinMQ::MQTT.subscription_arguments(opts(1))
      arguments[LavinMQ::MQTT::NO_LOCAL_HEADER]?.should be_nil
      arguments[LavinMQ::MQTT::RETAIN_AS_PUBLISHED_HEADER]?.should be_nil
    end

    it "carries No Local when set" do
      arguments = LavinMQ::MQTT.subscription_arguments(opts(1, no_local: true))
      arguments[LavinMQ::MQTT::QOS_HEADER].should eq 1u8
      arguments[LavinMQ::MQTT::NO_LOCAL_HEADER].should be_true
      arguments[LavinMQ::MQTT::RETAIN_AS_PUBLISHED_HEADER]?.should be_nil
    end

    it "carries Retain As Published when set" do
      arguments = LavinMQ::MQTT.subscription_arguments(opts(0, retain_as_published: true))
      arguments[LavinMQ::MQTT::QOS_HEADER].should eq 0u8
      arguments[LavinMQ::MQTT::RETAIN_AS_PUBLISHED_HEADER].should be_true
      arguments[LavinMQ::MQTT::NO_LOCAL_HEADER]?.should be_nil
    end

    it "carries both when both are set" do
      arguments = LavinMQ::MQTT.subscription_arguments(
        opts(1, no_local: true, retain_as_published: true))
      arguments[LavinMQ::MQTT::NO_LOCAL_HEADER].should be_true
      arguments[LavinMQ::MQTT::RETAIN_AS_PUBLISHED_HEADER].should be_true
    end

    it "allocates a fresh table only when an option is set" do
      # The default case must keep returning the shared constant, or every
      # subscription starts allocating on a path that never did.
      LavinMQ::MQTT.subscription_arguments(opts(1))
        .should be(LavinMQ::MQTT.subscription_arguments(opts(1)))
      LavinMQ::MQTT.subscription_arguments(opts(1, no_local: true))
        .should_not be(LavinMQ::MQTT.subscription_arguments(opts(1, no_local: true)))
    end
  end

  describe ".subscription_options" do
    it "round-trips everything subscription_arguments writes" do
      [opts(0), opts(1), opts(1, no_local: true),
       opts(0, retain_as_published: true),
       opts(1, no_local: true, retain_as_published: true)].each do |options|
        LavinMQ::MQTT.subscription_options(
          LavinMQ::MQTT.subscription_arguments(options)).should eq options
      end
    end

    it "grants QoS 2 as QoS 1" do
      LavinMQ::MQTT.subscription_options(table({LavinMQ::MQTT::QOS_HEADER => 2u8})).qos.should eq 1u8
    end

    it "accepts any integer type, not just UInt8" do
      LavinMQ::MQTT.subscription_options(table({LavinMQ::MQTT::QOS_HEADER => 1})).qos.should eq 1u8
      LavinMQ::MQTT.subscription_options(table({LavinMQ::MQTT::QOS_HEADER => 2i64})).qos.should eq 1u8
    end

    it "is all-defaults without arguments" do
      LavinMQ::MQTT.subscription_options(nil).should eq opts(0)
      LavinMQ::MQTT.subscription_options(LavinMQ::AMQP::Table.new).should eq opts(0)
    end

    it "is QoS 0 for a non-integer value" do
      LavinMQ::MQTT.subscription_options(table({LavinMQ::MQTT::QOS_HEADER => "1"})).qos.should eq 0u8
    end

    it "is QoS 0 for a negative value" do
      LavinMQ::MQTT.subscription_options(table({LavinMQ::MQTT::QOS_HEADER => -1})).qos.should eq 0u8
    end

    it "reads a non-true option value as false rather than raising" do
      # An operator or a definitions import can put anything here, and this runs
      # during load!, so a raise would be a boot failure.
      LavinMQ::MQTT.subscription_options(
        table({LavinMQ::MQTT::NO_LOCAL_HEADER => "yes"})).no_local?.should be_false
      LavinMQ::MQTT.subscription_options(
        table({LavinMQ::MQTT::NO_LOCAL_HEADER => 1})).no_local?.should be_false
      LavinMQ::MQTT.subscription_options(
        table({LavinMQ::MQTT::RETAIN_AS_PUBLISHED_HEADER => false})).retain_as_published?.should be_false
    end
  end
end
