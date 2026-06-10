require "./spec_helper"

private def connect_mqtt(port : Int32, client_id = "mqtt-server-spec")
  socket = TCPSocket.new("127.0.0.1", port, connect_timeout: 5)
  mqtt_io = MQTT::Protocol::IO.new(socket)
  MQTT::Protocol::Connect.new(
    client_id: client_id,
    clean_session: true,
    keepalive: 30_u16,
    username: "guest",
    password: "guest".to_slice,
    will: nil
  ).to_io(mqtt_io)
  connack = MQTT::Protocol::Packet.from_io(mqtt_io).as(MQTT::Protocol::Connack)
  connack.return_code.should eq MQTT::Protocol::Connack::ReturnCode::Accepted
  {socket, mqtt_io}
end

describe LavinMQ::MQTT::Server do
  it "accepts connections on a bound TCP socket" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    mqtt_server = LavinMQ::MQTT::Server.new(server)
    begin
      mqtt_server.bind_tcp(tcp_server)
      spawn(name: "mqtt direct listen spec") { mqtt_server.listen }
      wait_for { mqtt_server.@listeners.includes?(tcp_server) }

      mqtt_socket, mqtt_io = connect_mqtt(tcp_server.local_address.port, "mqtt-direct-listen-spec")
      MQTT::Protocol::Disconnect.new.to_io(mqtt_io)
    ensure
      mqtt_socket.try &.close
      mqtt_server.close
      server.close unless server.closed?
    end
  end

  it "accepts connections after the server restarts and the frontend is replaced" do
    tcp_server = TCPServer.new("127.0.0.1", 0)
    port = tcp_server.local_address.port
    server = LavinMQ::Server.new(LavinMQ::Config.instance)
    mqtt_server = LavinMQ::MQTT::Server.new(server)
    begin
      mqtt_server.bind_tcp(tcp_server)
      spawn(name: "mqtt restart listen spec") { mqtt_server.listen }
      wait_for { mqtt_server.@listeners.includes?(tcp_server) }

      mqtt_server.close
      restart_server(server)
      mqtt_server = LavinMQ::MQTT::Server.new(server)
      tcp_server = TCPServer.new("127.0.0.1", port)
      mqtt_server.bind_tcp(tcp_server)
      spawn(name: "mqtt restart replacement listen spec") { mqtt_server.listen }
      wait_for { mqtt_server.@listeners.includes?(tcp_server) }

      mqtt_socket, mqtt_io = connect_mqtt(tcp_server.local_address.port, "mqtt-restart-spec")
      MQTT::Protocol::Disconnect.new.to_io(mqtt_io)
    ensure
      mqtt_socket.try &.close
      mqtt_server.close
      server.close unless server.closed?
    end
  end
end
