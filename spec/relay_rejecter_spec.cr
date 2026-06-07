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

    # Regression: when TCP delivers the 8-byte protocol header in more than one
    # read, the rejecter must read the full header before comparing it, then
    # still send Connection.Start rather than closing the socket on a short read.
    it "handles a protocol header split across multiple reads" do
      tcp = TCPServer.new("localhost", 0)
      spawn(name: "amqp rejecter split spec") do
        while socket = tcp.accept?
          spawn LavinMQ::AMQP.reject_relay(socket)
        end
      end

      client = TCPSocket.new("localhost", tcp.local_address.port)
      header = Bytes[65, 77, 81, 80, 0, 0, 9, 1] # "AMQP" 0 0 9 1
      client.write header[0, 3]                  # first fragment, less than the full header
      client.flush
      sleep 50.milliseconds # give the server a chance to read the short fragment
      client.write header[3, header.size - 3]
      client.flush

      # Server must respond with a frame (Connection.Start, frame type 1), not EOF.
      client.read_timeout = 5.seconds
      client.read_byte.should eq 1_u8
    ensure
      client.try &.close
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
