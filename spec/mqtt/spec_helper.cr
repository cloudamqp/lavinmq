require "../spec_helper"
require "./spec_helper/mqtt_helpers"

def with_client_socket(server, &)
  listener = server.listeners.find { |l| l[:protocol] == :mqtt }.as(NamedTuple(ip_address: String, protocol: Symbol, port: Int32)
  )

  socket = TCPSocket.new(
    listener[:ip_address],
    listener[:port],
    connect_timeout: 30)
  socket.keepalive = true
  socket.tcp_nodelay = false
  socket.tcp_keepalive_idle = 60
  socket.tcp_keepalive_count = 3
  socket.tcp_keepalive_interval = 10
  socket.sync = true
  socket.read_buffering = true
  socket.buffer_size = 16384
  socket.read_timeout = 60.seconds
  yield socket
ensure
  socket.try &.close
end

def with_client_io(server, &)
  with_client_socket(server) do |socket|
    yield MQTT::Protocol::IO.new(socket), socket
  end
end

def with_mqtt_server(tls = false, replicator = LavinMQ::Clustering::NoopServer.new, &blk : LavinMQ::Server -> Nil)
  with_server(:mqtt, tls, replicator, &blk)
end
