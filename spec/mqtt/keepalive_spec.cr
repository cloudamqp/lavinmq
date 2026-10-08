require "./spec_helper"

module MqttSpecs
  extend MqttHelpers
  extend MqttMatchers

  describe "with keepalive" do
    it "client is disconnected after 1.5 * [keep alive] seconds", tags: "slow" do
      with_server do |server|
        with_client_io(server) do |io|
          connect(io, clean_session: false, keepalive: 1u16)
          sleep 1.6.seconds
          io.should be_closed
        end
      end
    end

    it "sends a v5 client DISCONNECT KeepAliveTimeout (0x8D) before closing", tags: "slow" do
      with_server do |server|
        with_client_socket(server) do |socket|
          socket.read_timeout = 3.seconds
          io = v5_connect(socket, keepalive: 1u16)
          pkt = MQTT::Protocol::Packet.from_io(io)
          pkt.should be_a(MQTT::Protocol::Disconnect)
          pkt.as(MQTT::Protocol::Disconnect).reason_code
            .should eq(MQTT::Protocol::Disconnect::ReasonCode::KeepAliveTimeout)
          io.should be_closed
        end
      end
    end
  end
end
