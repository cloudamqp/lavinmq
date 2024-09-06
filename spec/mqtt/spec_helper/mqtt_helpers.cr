require "mqtt-protocol"
require "./mqtt_client"

def packet_id_generator
  (0u16..).each
end

def next_packet_id
  packet_id_generator.next.as(UInt16)
end

def connect(io, expect_response = true, **args)
  MQTT::Protocol::Connect.new(**{
    client_id:     "client_id",
    clean_session: false,
    keepalive:     30u16,
    username:      "guest",
    password:      "guest".to_slice,
    will:          nil,
  }.merge(args)).to_io(io)
  MQTT::Protocol::Packet.from_io(io) if expect_response
end

def disconnect(io)
  MQTT::Protocol::Disconnect.new.to_io(io)
end

def mk_topic_filters(*args) : Array(MQTT::Protocol::Subscribe::TopicFilter)
  ret = Array(MQTT::Protocol::Subscribe::TopicFilter).new
  args.each { |topic, qos| ret << subtopic(topic, qos) }
  ret
end

def subscribe(io, expect_response = true, **args)
  MQTT::Protocol::Subscribe.new(**{packet_id: next_packet_id}.merge(args)).to_io(io)
  MQTT::Protocol::Packet.from_io(io) if expect_response
end

def unsubscribe(io, topics : Array(String), expect_response = true, packet_id = next_packet_id)
  MQTT::Protocol::Unsubscribe.new(topics, packet_id).to_io(io)
  MQTT::Protocol::Packet.from_io(io) if expect_response
end

def subtopic(topic : String, qos = 0)
  MQTT::Protocol::Subscribe::TopicFilter.new(topic, qos.to_u8)
end

def publish(io, expect_response = true, **args)
  pub_args = {
    packet_id: next_packet_id,
    payload:   "data".to_slice,
    dup:       false,
    qos:       0u8,
    retain:    false,
  }.merge(args)
  MQTT::Protocol::Publish.new(**pub_args).to_io(io)
  MQTT::Protocol::PubAck.from_io(io) if pub_args[:qos].positive? && expect_response
end

def puback(io, packet_id : UInt16?)
  return if packet_id.nil?
  MQTT::Protocol::PubAck.new(packet_id).to_io(io)
end

def ping(io)
  MQTT::Protocol::PingReq.new.to_io(io)
end

def read_packet(io)
  MQTT::Protocol::Packet.from_io(io)
end
