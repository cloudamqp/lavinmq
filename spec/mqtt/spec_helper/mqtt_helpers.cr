require "mqtt-protocol"
require "./mqtt_client"
require "../../spec_helper"

module MqttHelpers
  def with_client_socket(server, &)
    listener = server.listeners.find { |l| l[:protocol] == :mqtt }
    tcp_listener = listener.as(NamedTuple(ip_address: String, protocol: Symbol, port: Int32))

    socket = TCPSocket.new(
      tcp_listener[:ip_address],
      tcp_listener[:port],
      connect_timeout: 30)
    socket.keepalive = true
    socket.tcp_nodelay = false
    socket.tcp_keepalive_idle = 60
    socket.tcp_keepalive_count = 3
    socket.tcp_keepalive_interval = 10
    socket.sync = true
    socket.read_buffering = true
    socket.buffer_size = 16384
    socket.read_timeout = 1.seconds
    yield socket
  ensure
    socket.try &.close
  end

  def with_server(tls = false, replicator = LavinMQ::Clustering::NoopServer.new, &blk : LavinMQ::Server -> Nil)
    with_server(:mqtt, tls, replicator) do |s|
      yield s
    end
  end

  def with_client_io(server, &)
    with_client_socket(server) do |socket|
      io = MQTT::Protocol::IO.new(socket)
      with MqttHelpers yield io
    end
  end

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
end
