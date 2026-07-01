require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers
  describe "MQTT 5.0 publish" do
    it "answers a QoS 2 publish with PUBREC and keeps the connection open" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = MQTT::Protocol::IO::V5.new(socket)
          connect(io, version: MQTT::Protocol::Version::V5)

          # The CONNACK carries no Maximum QoS, which means 2 (3.2.2.3.4).
          MQTT::Protocol::Publish.new(
            topic: "test/topic", payload: "x".to_slice,
            packet_id: 1u16, dup: false, qos: 2u8, retain: false,
          ).to_io(io)
          io.flush

          pubrec = MQTT::Protocol::Packet.from_io(io).as(MQTT::Protocol::PubRec)
          pubrec.packet_id.should eq 1u16
        end
      end
    end
  end
end
