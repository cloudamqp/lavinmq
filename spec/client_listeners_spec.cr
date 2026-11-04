require "./spec_helper"
require "../src/lavinmq/client_listeners"

# The leader runs on the same host in these specs, so it can't use the same
# ports: forwards to *leader_ports*, by this node's port
private class SpecListeners < LavinMQ::ClientListeners
  def initialize(config, leader_host, @leader_ports : Hash(Int32, Int32))
    super(config, leader_host)
  end

  private def leader_port(l) : Int32?
    l.target_port.try { |port| @leader_ports[port] }
  end
end

# Reports whether what it accepts has TCP_NODELAY set
private class NodelayListeners < LavinMQ::ClientListeners
  getter nodelay = Channel(Bool).new(1)

  private def dispatch(l, io) : Nil
    nodelay.send io.as(TCPSocket).tcp_nodelay?
    io.close
  end
end

private def tls_context : OpenSSL::SSL::Context::Server
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  ctx
end

private def tls_client : OpenSSL::SSL::Context::Client
  ctx = OpenSSL::SSL::Context::Client.new
  ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  ctx
end

# Only AMQP and AMQPS, on ports of their own
private def listener_config : LavinMQ::Config
  config = LavinMQ::Config.new
  config.amqp_bind = "127.0.0.1"
  config.amqp_port = free_port
  config.amqps_port = free_port
  config.mqtt_port = config.mqtts_port = config.http_port = config.https_port = -1
  config.unix_path = config.mqtt_unix_path = config.http_unix_path = ""
  config
end

private def proxied_connections(s : LavinMQ::Server) : Array(Bool)
  s.connections.map { |c| c.as(LavinMQ::AMQP::Client).connection_info.proxied? }
end

describe LavinMQ::ClientListeners do
  it "forwards AMQP and AMQPS to the leader, with the client's address, until this node serves" do
    with_amqp_server(extra_tls_listener: true) do |s|
      with_proxy_protocol do # how the leader trusts a follower's PROXY header
        s.@config.default_user_only_loopback = false
        config = listener_config
        leader : String? = "localhost"
        listeners = SpecListeners.new(config, -> { leader.as(String?) },
          {config.amqp_port => amqp_port(s), config.amqps_port => amqp_tls_port(s)})
        listeners.bind(tls_context, nil, nil)
        listeners.listen
        begin
          plain = AMQP::Client.new(host: "127.0.0.1", port: config.amqp_port).connect
          tls = AMQP::Client.new(host: "127.0.0.1", port: config.amqps_port, tls: tls_client).connect
          plain.channel.queue("through-follower").publish_confirm("m").should be_true
          tls.channel.queue("through-follower").publish_confirm("m").should be_true
          s.vhosts["/"].queue("through-follower").message_count.should eq 2
          proxied_connections(s).should eq [true, true]

          # Now this node serves, on the same ports
          listeners.serve(LavinMQ::ClientListeners::Brokers.new(s.amqp_server, s.mqtt_server, s.http_server))
          wait_for { plain.closed? && tls.closed? } # were forwarded to the old leader
          local = AMQP::Client.new(host: "127.0.0.1", port: config.amqp_port).connect
          local.channel.queue("through-follower").publish_confirm("m").should be_true
          proxied_connections(s).should eq [false]
          local.close
        ensure
          listeners.close
        end
      end
    end
  end

  it "hands HTTP to the broker with the client's address" do
    with_amqp_server do |s|
      config = listener_config
      config.amqp_port = config.amqps_port = -1
      config.http_bind = "127.0.0.1"
      config.http_port = free_port
      listeners = LavinMQ::ClientListeners.new(config, -> { nil.as(String?) })
      listeners.bind(nil, nil, nil)
      listeners.listen
      listeners.serve(LavinMQ::ClientListeners::Brokers.new(s.amqp_server, s.mqtt_server, s.http_server))
      # The default user may only log in from loopback
      response = HTTP::Client.new("127.0.0.1", config.http_port).get("/api/whoami",
        headers: HTTP::Headers{"Authorization" => "Basic #{Base64.strict_encode("guest:guest")}"})
      response.status_code.should eq 200
    ensure
      listeners.try &.close
    end
  end

  it "accepts with TCP_NODELAY set, as configured" do
    config = listener_config
    config.amqp_port = config.amqps_port = -1
    config.http_bind = "127.0.0.1"
    config.http_port = free_port
    listeners = NodelayListeners.new(config, -> { nil.as(String?) })
    listeners.bind(nil, nil, nil)
    listeners.listen
    TCPSocket.new("127.0.0.1", config.http_port).close
    listeners.nodelay.receive.should be_true
  ensure
    listeners.try &.close
  end

  it "lets connections wait until it serves, without listening before" do
    with_amqp_server do |s|
      config = listener_config
      config.amqps_port = -1
      listeners = LavinMQ::ClientListeners.new(config, -> { nil.as(String?) })
      listeners.bind(nil, nil, nil)
      connected = Channel(AMQP::Client::Connection).new(1)
      spawn { connected.send AMQP::Client.new(host: "127.0.0.1", port: config.amqp_port).connect }
      sleep 50.milliseconds # in the listen backlog, not closed
      listeners.serve(LavinMQ::ClientListeners::Brokers.new(s.amqp_server, s.mqtt_server, s.http_server))
      select
      when conn = connected.receive
        conn.channel.queue("waited").publish_confirm("m").should be_true
        conn.close
      when timeout(5.seconds)
        fail "the waiting connection wasn't served"
      end
    ensure
      listeners.try &.close
    end
  end

  it "closes the connection of a forwarded client that stops reading for tcp_send_timeout" do
    config = listener_config
    config.amqps_port = -1
    config.tcp_send_timeout = 1
    leader = TCPServer.new("127.0.0.1", 0)
    listeners = SpecListeners.new(config, -> { "127.0.0.1".as(String?) },
      {config.amqp_port => leader.local_address.port})
    listeners.bind(nil, nil, nil)
    listeners.listen
    client = TCPSocket.new("127.0.0.1", config.amqp_port)
    upstream = leader.accept
    # The leader keeps sending to a client that never reads, until the
    # follower gives up on the client and closes the connection to the
    # leader too
    closed = Channel(Exception).new(1)
    spawn(name: "leader writing to a stalled client") do
      chunk = Bytes.new(64 * 1024)
      loop { upstream.write chunk }
    rescue ex
      closed.send ex
    end
    select
    when ex = closed.receive
      ex.should be_a(IO::Error)
    when timeout(10.seconds)
      fail "The follower kept the connection of a client that doesn't read"
    end
  ensure
    client.try &.close rescue nil
    upstream.try &.close rescue nil
    listeners.try &.close
    leader.try &.close
  end

  it "closes connections while neither serving nor knowing of a leader" do
    config = listener_config
    listeners = LavinMQ::ClientListeners.new(config, -> { nil.as(String?) })
    listeners.bind(nil, nil, nil)
    listeners.listen
    socket = TCPSocket.new("127.0.0.1", config.amqp_port)
    socket.read_timeout = 5.seconds
    socket.read_byte.should be_nil
  ensure
    socket.try &.close
    listeners.try &.close
  end
end
