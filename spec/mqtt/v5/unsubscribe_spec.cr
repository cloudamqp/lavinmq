require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 unsubscribe" do
    it "UNSUBACK carries a reason code per topic filter, in order [MQTT-3.11.3-1]" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)
          subscribe(io, topic_filters: [subtopic("a/b", 0)], packet_id: 1u16)

          # "a/b" was subscribed -> Success (0x00); "x/y" never was ->
          # NoSubscriptionExisted (0x11). Order matches the topic filters.
          unsuback = unsubscribe(io, topics: ["a/b", "x/y"], packet_id: 2u16)
            .as(MQTT::Protocol::UnsubAck)
          unsuback.packet_id.should eq(2u16)
          unsuback.reason_codes.should eq([
            MQTT::Protocol::UnsubAck::ReasonCode::Success,
            MQTT::Protocol::UnsubAck::ReasonCode::NoSubscriptionExisted,
          ])
        end
      end
    end
  end
end
