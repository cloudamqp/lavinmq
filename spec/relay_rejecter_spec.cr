require "./spec_helper"
require "../src/lavinmq/amqp/relay_rejecter"
require "../src/lavinmq/mqtt/relay_rejecter"

# A DR relay node must not serve application clients, but rather than leaving the
# ports closed it rejects connections at the protocol level so clients get a
# clear reason and disconnect cleanly.
describe "relay client rejecters" do
  describe "LavinMQ::AMQP.reject_relay" do
    it "closes an AMQP connection with reason 'server in relay mode'" do
      tcp = TCPServer.new("localhost", 0)
      spawn(name: "amqp rejecter spec") do
        while socket = tcp.accept?
          spawn LavinMQ::AMQP.reject_relay(socket)
        end
      end

      expect_raises(AMQP::Client::Connection::ClosedException, /server in relay mode/) do
        AMQP::Client.new(host: "localhost", port: tcp.local_address.port).connect
      end
    ensure
      tcp.try &.close
    end
  end

  describe "LavinMQ::MQTT.reject_relay" do
    it "replies CONNACK ServerUnavailable to an MQTT CONNECT" do
      tcp = TCPServer.new("localhost", 0)
      spawn(name: "mqtt rejecter spec") do
        while socket = tcp.accept?
          spawn LavinMQ::MQTT.reject_relay(socket, 8192_u32)
        end
      end

      client = TCPSocket.new("localhost", tcp.local_address.port)
      # Minimal MQTT 3.1.1 CONNECT: protocol "MQTT" v4, clean session, empty client id.
      connect = Bytes[0x10, 0x0C,
        0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, # "MQTT"
        0x04,                               # protocol level 4 (3.1.1)
        0x02,                               # connect flags: clean session
        0x00, 0x3C,                         # keepalive 60s
        0x00, 0x00]                         # empty client id
      client.write connect
      client.flush

      connack = Bytes.new(4)
      client.read_fully(connack)
      connack[0].should eq 0x20_u8                                                      # CONNACK packet type
      connack[3].should eq MQTT::Protocol::Connack::ReturnCode::ServerUnavailable.value # return code 3
    ensure
      client.try &.close
      tcp.try &.close
    end
  end
end
