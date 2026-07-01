require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 connect" do
    it "negotiates the protocol version from CONNECT and replies with a v5 CONNACK" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          # A v5 CONNECT must be answered with a v5-framed CONNACK; if the
          # broker kept v3 framing the reply would be unparseable here.
          connack = connect(io, version: MQTT::Protocol::Version::V5)
          connack.should be_a(MQTT::Protocol::Connack)
          connack = connack.as(MQTT::Protocol::Connack)
          connack.reason_code.should eq(MQTT::Protocol::Connack::ReasonCode::Success)
        end
      end
    end

    it "advertises server capabilities in the v5 CONNACK" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connack = connect(io, version: MQTT::Protocol::Version::V5).as(MQTT::Protocol::Connack)
          props = connack.properties
          props.maximum_qos.should eq(1u8)
          props.retain_available.should be_true
          props.wildcard_subscription_available.should be_true
          props.topic_alias_maximum.should eq(0u16)
          props.subscription_identifier_available.should be_false
          props.shared_subscription_available.should be_false
          props.maximum_packet_size.should eq(LavinMQ::Config.instance.mqtt_max_packet_size)
        end
      end
    end
  end
end
