require "./spec_helper"

describe LavinMQ::MQTT::SubscriptionKey do
  describe "#arguments" do
    it "returns the QoS 0 constant for QoS 0" do
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 0u8).arguments
        .should be(LavinMQ::MQTT::QOS0_ARGUMENTS)
    end

    it "returns the QoS 1 constant for QoS 1" do
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 1u8).arguments
        .should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end

    it "returns the QoS 1 constant for QoS 2" do
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 2u8).arguments
        .should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end
  end

  describe "#arguments with subscription options" do
    # This is the definitions-compaction guard. `compact!` does not replay the
    # original Queue::Bind frames, it re-derives every binding from
    # `bindings_details` -> `SubscriptionKey#arguments`, so anything this
    # method cannot reconstruct survives a plain restart and then vanishes at
    # the first compaction.
    it "renders No Local" do
      key = LavinMQ::MQTT::SubscriptionKey.new("a/b",
        LavinMQ::MQTT::SubscriptionOptions.new(1u8, no_local: true))
      key.arguments.not_nil![LavinMQ::MQTT::NO_LOCAL_HEADER].should be_true
    end

    it "renders Retain As Published" do
      key = LavinMQ::MQTT::SubscriptionKey.new("a/b",
        LavinMQ::MQTT::SubscriptionOptions.new(1u8, retain_as_published: true))
      key.arguments.not_nil![LavinMQ::MQTT::RETAIN_AS_PUBLISHED_HEADER].should be_true
    end

    it "round-trips through subscription_options" do
      options = LavinMQ::MQTT::SubscriptionOptions.new(1u8,
        no_local: true, retain_as_published: true)
      key = LavinMQ::MQTT::SubscriptionKey.new("a/b", options)
      LavinMQ::MQTT.subscription_options(key.arguments).should eq options
    end

    it "still returns the shared constant for a subscription with no options" do
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 1u8).arguments
        .should be(LavinMQ::MQTT::QOS1_ARGUMENTS)
    end
  end

  describe "#properties_key" do
    it "returns ~ for an empty topic filter" do
      LavinMQ::MQTT::SubscriptionKey.new("", 0u8).properties_key.should eq "~"
      LavinMQ::MQTT::SubscriptionKey.new("", 1u8).properties_key.should eq "~"
      LavinMQ::MQTT::SubscriptionKey.new("", 2u8).properties_key.should eq "~"
    end

    it "appends the QoS" do
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 0u8).properties_key.should eq "a/b~0"
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 1u8).properties_key.should eq "a/b~1"
      LavinMQ::MQTT::SubscriptionKey.new("a/b", 2u8).properties_key.should eq "a/b~2"
    end
  end

  it "exposes the topic filter as #topic_filter, aliased by #routing_key" do
    key = LavinMQ::MQTT::SubscriptionKey.new("a/+/c", 1u8)
    key.topic_filter.should eq "a/+/c"
    key.routing_key.should eq "a/+/c"
  end

  it "matches the LavinMQ::AMQP::BindingKey interface (duck typing)" do
    sub = LavinMQ::MQTT::SubscriptionKey.new("a/b", 1u8)
    amqp = LavinMQ::AMQP::BindingKey.new("a/b", LavinMQ::MQTT::QOS1_ARGUMENTS)
    sub.routing_key.should eq amqp.routing_key
    sub.arguments.should eq amqp.arguments
    # Both expose a String properties_key (the encoding schemes differ by design).
    sub.properties_key.should be_a(String)
    amqp.properties_key.should be_a(String)
  end

  it "uses value semantics (topic filter + QoS) as a Hash key" do
    h = Hash(LavinMQ::MQTT::SubscriptionKey, Int32).new
    h[LavinMQ::MQTT::SubscriptionKey.new("a/b", 1u8)] = 1
    h[LavinMQ::MQTT::SubscriptionKey.new("a/b", 1u8)] = 2
    h.size.should eq 1
    h[LavinMQ::MQTT::SubscriptionKey.new("a/b", 2u8)] = 3 # same topic, different QoS
    h.size.should eq 2
  end
end
