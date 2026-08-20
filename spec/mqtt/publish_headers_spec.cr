require "./spec_helper"

module MqttSpecs
  extend MqttHelpers

  # `restore` is not only fed headers that `store` wrote: definitions_store
  # resolves a bind target as `@queues[name]? || @sessions[name]?`, so an AMQP
  # client can bind `mqtt.<client-id>` to `amq.topic` and publish anything. A
  # raise in here is requeued and re-raised by Session#get_packet, which poisons
  # the queue permanently, so every field has to degrade instead.
  describe LavinMQ::MQTT::PublishHeaders do
    describe ".restore" do
      it "drops a message_expiry_interval that is negative" do
        headers = LavinMQ::AMQP::Table.new({"mqtt.message_expiry_interval" => -1_i32})
        props = LavinMQ::MQTT::PublishHeaders.restore(headers)
        props.message_expiry_interval.should be_nil
      end

      it "drops a message_expiry_interval that overflows UInt32" do
        headers = LavinMQ::AMQP::Table.new({"mqtt.message_expiry_interval" => Int64::MAX})
        props = LavinMQ::MQTT::PublishHeaders.restore(headers)
        props.message_expiry_interval.should be_nil
      end

      it "keeps a valid message_expiry_interval" do
        headers = LavinMQ::AMQP::Table.new({"mqtt.message_expiry_interval" => 3600_i32})
        props = LavinMQ::MQTT::PublishHeaders.restore(headers)
        props.message_expiry_interval.should eq 3600u32
      end

      it "drops a response_topic containing wildcards [MQTT-3.3.2-14]" do
        %w[a/# a/+/b # +].each do |topic|
          headers = LavinMQ::AMQP::Table.new({"mqtt.response_topic" => topic})
          LavinMQ::MQTT::PublishHeaders.restore(headers).response_topic.should be_nil
        end
      end

      it "keeps a wildcard-free response_topic" do
        headers = LavinMQ::AMQP::Table.new({"mqtt.response_topic" => "reply/here"})
        LavinMQ::MQTT::PublishHeaders.restore(headers).response_topic.should eq "reply/here"
      end

      it "round-trips what store wrote" do
        stored = MQTT::Protocol::PublishProperties.new
        stored.payload_format_indicator = true
        stored.message_expiry_interval = 120u32
        stored.response_topic = "reply/here"
        stored.correlation_data = Bytes[1, 2, 3]
        stored.content_type = "application/json"
        stored.user_properties = [{"a", "1"}, {"b", "2"}, {"a", "3"}]

        headers = LavinMQ::AMQP::Table.new
        LavinMQ::MQTT::PublishHeaders.store(stored, headers)
        LavinMQ::MQTT::PublishHeaders.restore(headers).should eq stored
      end
    end
  end
end
