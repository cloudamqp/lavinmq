require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 publish" do
    it "passes v5 PUBLISH properties through to a v5 subscriber, preserving user-property order" do
      with_server do |server|
        with_client_socket(server) do |sub_socket|
          sub = MQTT::Protocol::IO::V5.new(sub_socket)
          connect(sub, version: MQTT::Protocol::Version::V5, client_id: "sub")
          subscribe(sub, topic_filters: [subtopic("test/topic", 1)], packet_id: 1u16)

          props = MQTT::Protocol::PublishProperties.new
          props.payload_format_indicator = true
          props.message_expiry_interval = 3600u32
          props.response_topic = "reply/here"
          props.correlation_data = Bytes[1, 2, 3]
          props.content_type = "application/json"
          # order + a duplicate key, both of which must survive [MQTT-3.3.2-17/18]
          props.user_properties = [{"a", "1"}, {"b", "2"}, {"a", "3"}]

          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO::V5.new(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            MQTT::Protocol::Publish.new(
              topic: "test/topic", payload: "hello".to_slice,
              packet_id: 2u16, dup: false, qos: 1u8, retain: false, properties: props,
            ).to_io(pub)
            pub.flush
            MQTT::Protocol::Packet.from_io(pub).should be_a(MQTT::Protocol::PubAck)
          end

          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          dp = delivered.properties
          dp.payload_format_indicator.should be_true
          dp.message_expiry_interval.should eq(3600u32)
          dp.response_topic.should eq("reply/here")
          dp.correlation_data.should eq(Bytes[1, 2, 3])
          dp.content_type.should eq("application/json")
          dp.user_properties.should eq([{"a", "1"}, {"b", "2"}, {"a", "3"}])
        end
      end
    end

    it "delivers a v5-published message to a v3 subscriber without leaking v5 properties" do
      with_server do |server|
        with_client_io(server) do |sub| # v3 subscriber
          connect(sub)
          subscribe(sub, topic_filters: [subtopic("test/topic", 1)], packet_id: 1u16)

          props = MQTT::Protocol::PublishProperties.new
          props.content_type = "application/json"
          props.user_properties = [{"a", "1"}]
          with_client_socket(server) do |pub_socket|
            pub = MQTT::Protocol::IO::V5.new(pub_socket)
            connect(pub, version: MQTT::Protocol::Version::V5, client_id: "pub")
            MQTT::Protocol::Publish.new(
              topic: "test/topic", payload: "hi".to_slice,
              packet_id: 2u16, dup: false, qos: 1u8, retain: false, properties: props,
            ).to_io(pub)
            pub.flush
            MQTT::Protocol::Packet.from_io(pub).should be_a(MQTT::Protocol::PubAck)
          end

          # v3 framing carries no properties section; the packet must still be
          # well-formed with the right payload/topic (properties must not corrupt it).
          delivered = MQTT::Protocol::Packet.from_io(sub).as(MQTT::Protocol::Publish)
          String.new(delivered.payload).should eq("hi")
          delivered.topic.should eq("test/topic")
          delivered.properties.user_properties.should be_empty
        end
      end
    end

    it "disconnects with QoSNotSupported (0x9B) when a client publishes QoS 2" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised maximum_qos=1, so a QoS 2 PUBLISH is a protocol error
          # [MQTT-3.2.2] -> the server must answer with DISCONNECT 0x9B and close.
          MQTT::Protocol::Publish.new(
            topic: "test/topic", payload: "x".to_slice,
            packet_id: 1u16, dup: false, qos: 2u8, retain: false,
          ).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::QoSNotSupported)
        end
      end
    end

    it "disconnects with TopicAliasInvalid (0x94) when a client sends a Topic Alias" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised topic_alias_maximum=0, so any Topic Alias is invalid.
          props = MQTT::Protocol::PublishProperties.new
          props.topic_alias = 1u16
          MQTT::Protocol::Publish.new(
            topic: "test/topic", payload: "x".to_slice,
            packet_id: 1u16, dup: false, qos: 1u8, retain: false, properties: props,
          ).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::TopicAliasInvalid)
        end
      end
    end

    it "disconnects with ProtocolError (0x82) on an empty topic with no alias" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The shard refuses to encode an empty-topic PUBLISH, so send raw bytes
          # for a v5 QoS 0 PUBLISH with an empty topic, empty properties, payload
          # "x": [0x30, remaining=4, topic-len=0x0000, props-len=0x00, 'x'].
          io.write_bytes_raw(Bytes[0x30, 0x04, 0x00, 0x00, 0x00, 0x78])
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::ProtocolError)
        end
      end
    end
  end
end
