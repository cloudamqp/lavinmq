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

    {
      "packet type 0"                => Bytes[0x00, 0x00],
      "PINGREQ with reserved flag 1" => Bytes[0xC1, 0x00],
      "PUBLISH with QoS 3"           => Bytes[0x36, 0x04, 0x00, 0x01, 0x61, 0x00],
      "wildcard in a topic name"     => Bytes[0x30, 0x07, 0x00, 0x03, 0x61, 0x2F, 0x2B, 0x00, 0x78],
      "invalid topic filter a/#/b"   => Bytes[0x82, 0x0B, 0x00, 0x01, 0x00, 0x00, 0x05, 0x61, 0x2F, 0x23, 0x2F, 0x62, 0x00],
    }.each do |desc, bytes|
      it "answers DISCONNECT MalformedPacket (0x81) to #{desc}" do
        with_server do |server|
          with_client_socket(server) do |socket|
            io = v5_connect(socket)
            io.write_bytes_raw(bytes)
            io.flush

            pkt = MQTT::Protocol::Packet.from_io(io)
            pkt.should be_a(MQTT::Protocol::Disconnect)
            pkt.as(MQTT::Protocol::Disconnect).reason_code
              .should eq(MQTT::Protocol::Disconnect::ReasonCode::MalformedPacket)
          end
        end
      end
    end
  end
end
