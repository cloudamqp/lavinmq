require "../spec_helper"

module MqttSpecs
  extend MqttHelpers
  describe "ping" do
    it "responds to ping [MQTT-3.12.4-1]" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io)
          ping(io)
          resp = read_packet(io)
          resp.should be_a(MQTT::Protocol::PingResp)
        end
      end
    end
  end
end
