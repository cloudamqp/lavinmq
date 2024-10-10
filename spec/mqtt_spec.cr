require "spec"
require "socket"
require "./spec_helper"
require "mqtt-protocol"
require "../src/lavinmq/mqtt/connection_factory"

def setup_connection(s, pass)
  left, right = UNIXSocket.pair
  io = MQTT::Protocol::IO.new(left)
  s.users.create("usr", "pass", [LavinMQ::Tag::Administrator])
  MQTT::Protocol::Connect.new("abc", false, 60u16, "usr", pass.to_slice, nil).to_io(io)
  connection_factory = LavinMQ::MQTT::ConnectionFactory.new(
    s.users,
    s.vhosts["/"],
    LavinMQ::MQTT::Broker.new(s.vhosts["/"]))
  {connection_factory.start(right, LavinMQ::ConnectionInfo.local), io}
end

describe LavinMQ do
  it "MQTT connection should pass authentication" do
    with_amqp_server do |s|
      client, io = setup_connection(s, "pass")
      client.should be_a(LavinMQ::MQTT::Client)
      # client.close
      MQTT::Protocol::Disconnect.new.to_io(io)
    end
  end

  it "unauthorized MQTT connection should not pass authentication" do
    with_amqp_server do |s|
      client, io = setup_connection(s, "pa&ss")
      client.should_not be_a(LavinMQ::MQTT::Client)
      # client.close
      MQTT::Protocol::Disconnect.new.to_io(io)
    end
  end

  it "should handle a Ping" do
    with_amqp_server do |s|
      client, io = setup_connection(s, "pass")
      client.should be_a(LavinMQ::MQTT::Client)
      MQTT::Protocol::PingReq.new.to_io(io)
      MQTT::Protocol::Packet.from_io(io).should be_a(MQTT::Protocol::Connack)
      MQTT::Protocol::Packet.from_io(io).should be_a(MQTT::Protocol::PingResp)
    end
  end
end
