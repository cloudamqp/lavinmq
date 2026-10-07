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

  describe "MQTT 5.0 unexpected packets" do
    it "answers DISCONNECT ProtocolError (0x82) to a client-sent PINGRESP" do
      with_server do |server|
        with_client_socket(server) do |socket|
          io = v5_connect(socket)

          # PINGRESP is server-to-client only, so a client sending one is a
          # protocol error rather than something we should log a backtrace for.
          MQTT::Protocol::PingResp.new.to_io(io)
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
