require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 subscribe" do
    it "disconnects with SubscriptionIdentifiersNotSupported (0xA1) on a Subscription Identifier" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # We advertised subscription_identifier_available=0, so any Subscription
          # Identifier is a protocol error -> DISCONNECT 0xA1.
          props = MQTT::Protocol::SubscribeProperties.new
          props.subscription_identifier = 1u32
          tf = MQTT::Protocol::Subscribe::TopicFilter.new("test/topic", 0u8)
          MQTT::Protocol::Subscribe.new([tf], 1u16, props).to_io(io)
          io.flush

          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::SubscriptionIdentifiersNotSupported)
        end
      end
    end
  end
end
