require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 publish" do
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

          # An empty topic is only valid with an alias to resolve it; we allow none.
          MQTT::Protocol::Publish.new(
            topic: "", payload: "x".to_slice,
            packet_id: 1u16, dup: false, qos: 1u8, retain: false,
          ).to_io(io)
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
