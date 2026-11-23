require "./spec_helper"
require "../src/lavinmq/client_listeners"

private alias Leader = LavinMQ::Clustering::Client::Leader

# A leader on this host, which can't use this node's ports, so it advertises
# its own, as a leader does in its URI (see Clustering::Runner.advertised_uri)
private def leader_at(host : String, ports : Hash(String, Int32)) : Leader?
  Leader.new(host, 5679, ports).as(Leader?)
end

# The ports it would forward to on *leader*, one per listener
private class PortListeners < LavinMQ::ClientListeners
  def leader_ports(leader : LavinMQ::Clustering::Client::Leader) : Array(Int32?)
    @listeners.map { |l| leader_port(l, leader) }
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

private AMQP_START = "AMQP\u0000\u0000\t\u0001"

# Sends *payload* and the AMQP protocol header through a follower's AMQP
# port (AMQPS with *tls*) to a fake leader, returns what the leader got in
# front of the AMQP protocol header, and the client's address
private def forward_through_follower(payload : Bytes, *, tls = false, trusted = "", &configure : LavinMQ::Config -> _)
  config = listener_config
  config.tcp_proxy_protocol = true
  config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list(trusted) unless trusted.empty?
  configure.call(config)
  leader = TCPServer.new("127.0.0.1", 0)
  listeners = LavinMQ::ClientListeners.new(config, -> { leader_at("127.0.0.1", {"amqp" => leader.local_address.port}) })
  listeners.bind(tls_context, nil, nil)
  listeners.listen
  socket = TCPSocket.new("127.0.0.1", tls ? config.amqps_port : config.amqp_port)
  client_address = socket.local_address
  # A load balancer's header comes before TLS
  socket.write payload
  client = tls ? OpenSSL::SSL::Socket::Client.new(socket, tls_client, sync_close: true) : socket
  client.write AMQP_START.to_slice
  client.flush
  upstream = leader.accept
  upstream.read_timeout = 5.seconds
  received = IO::Memory.new
  buf = Bytes.new(256)
  until received.size >= AMQP_START.bytesize && received.to_slice[-AMQP_START.bytesize..] == AMQP_START.to_slice
    n = upstream.read(buf)
    raise "Leader connection closed" if n.zero?
    received.write buf[0, n]
  end
  {received.to_slice[0, received.size - AMQP_START.bytesize], client_address}
ensure
  client.try &.close rescue nil
  upstream.try &.close rescue nil
  listeners.try &.close
  leader.try &.close
end

private def forward_through_follower(payload : Bytes, *, tls = false, trusted = "")
  forward_through_follower(payload, tls: tls, trusted: trusted) { }
end

private def parse_header(bytes : Bytes)
  r, w = IO.pipe
  w.write bytes
  w.close
  LavinMQ::ProxyProtocol.read(r).should_not be_nil
end

# A PROXY v2 header from a load balancer, with an ALPN TLV and an SSL TLV
private def proxy_v2_header_with_tlvs : Bytes
  header = LavinMQ::ProxyProtocol::V2.new(
    Socket::IPAddress.new("203.0.113.7", 40000),
    Socket::IPAddress.new("10.0.0.1", 5672),
    [LavinMQ::ProxyProtocol::V2::TLV.new(0x01u8, "amqp".to_slice),
     LavinMQ::ProxyProtocol::V2.ssl_tlv("TLSv1.2", "lb-cipher")])
  io = IO::Memory.new
  io.write_bytes header, IO::ByteFormat::NetworkEndian
  io.to_slice
end

private LB_V1_HEADER  = "PROXY TCP4 203.0.113.7 10.0.0.1 40000 5672\r\n"
private OWN_V1_HEADER = /\APROXY TCP4 127\.0\.0\.1 127\.0\.0\.1 (\d+) \d+\r\n\z/

private def proxied_connections(s : LavinMQ::Server) : Array(Bool)
  s.connections.map { |c| c.as(LavinMQ::AMQP::Client).connection_info.proxied? }
end

private def tls_connections(s : LavinMQ::Server) : Array(String)
  s.connections.compact_map do |c|
    info = c.as(LavinMQ::AMQP::Client).connection_info
    info.ssl_version if info.ssl?
  end
end

describe LavinMQ::ClientListeners do
  it "forwards AMQP and AMQPS to the leader, with the client's address, until this node serves" do
    # The leader has no TLS port: this node terminates TLS, with its own
    # certificate, and forwards to the leader's plain port
    with_amqp_server do |s|
      with_proxy_protocol do # how the leader trusts a follower's PROXY header
        s.@config.default_user_only_loopback = false
        config = listener_config
        listeners = LavinMQ::ClientListeners.new(config, -> { leader_at("localhost", {"amqp" => amqp_port(s)}) })
        listeners.bind(tls_context, nil, nil)
        listeners.listen
        begin
          plain = AMQP::Client.new(host: "127.0.0.1", port: config.amqp_port).connect
          tls = AMQP::Client.new(host: "127.0.0.1", port: config.amqps_port, tls: tls_client).connect
          plain.channel.queue("through-follower").publish_confirm("m").should be_true
          tls.channel.queue("through-follower").publish_confirm("m").should be_true
          s.vhosts["/"].queue("through-follower").message_count.should eq 2
          proxied_connections(s).should eq [true, true]
          # The TLS session this node terminated, from the PROXY header
          tls_connections(s).size.should eq 1
          tls_connections(s).first.should start_with "TLSv1."

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

  it "forwards to this node's own port when the leader doesn't advertise it" do
    config = listener_config
    config.amqp_port = -1 # AMQPS passes TLS through, to the leader's AMQPS port
    listeners = PortListeners.new(config, -> { nil.as(Leader?) })
    listeners.bind(tls_context, nil, nil)
    listeners.leader_ports(Leader.new("leader", 5679)).should eq [config.amqps_port]
    listeners.leader_ports(Leader.new("leader", 5679, {"amqps" => 30671})).should eq [30671]
  ensure
    listeners.try &.close
  end

  it "passes TLS through to the leader's TLS port when there's no plain port" do
    with_amqp_server(extra_tls_listener: true) do |s|
      with_proxy_protocol do
        s.@config.default_user_only_loopback = false
        config = listener_config
        config.amqp_port = -1
        listeners = LavinMQ::ClientListeners.new(config, -> { leader_at("localhost", {"amqps" => amqp_tls_port(s)}) })
        listeners.bind(tls_context, nil, nil)
        listeners.listen
        begin
          tls = AMQP::Client.new(host: "127.0.0.1", port: config.amqps_port, tls: tls_client).connect
          tls.channel.queue("passed-through").publish_confirm("m").should be_true
          proxied_connections(s).should eq [true]
          tls_connections(s).size.should eq 1 # terminated by the leader
          tls.close
        ensure
          listeners.close
        end
      end
    end
  end

  describe "with tcp_proxy_protocol" do
    it "replaces the PROXY header of a trusted source with one header for the original client" do
      header, _ = forward_through_follower(LB_V1_HEADER.to_slice, trusted: "127.0.0.1")
      String.new(header).should eq LB_V1_HEADER
    end

    it "forwards the PROXY v2 header of a trusted source as v2, TLVs included" do
      v2_header = proxy_v2_header_with_tlvs
      header, _ = forward_through_follower(v2_header)
      header.should eq v2_header
    end

    it "strips the PROXY header of an untrusted source" do
      {LB_V1_HEADER.to_slice, proxy_v2_header_with_tlvs}.each do |lb_header|
        header, client_address = forward_through_follower(lb_header, trusted: "10.9.9.9")
        String.new(header).match(OWN_V1_HEADER).should_not be_nil
        String.new(header).should contain " #{client_address.port} "
      end
    end

    it "sends its own header in place of a LOCAL header" do
      io = IO::Memory.new
      local = LavinMQ::ProxyProtocol::V2.new(nil, nil, command: LavinMQ::ProxyProtocol::V2::Command::Local)
      io.write_bytes local, IO::ByteFormat::NetworkEndian
      header, _ = forward_through_follower(io.to_slice)
      String.new(header).match(OWN_V1_HEADER).should_not be_nil
    end

    it "forwards data that came with the header, also beyond the read buffer used for it" do
      data = Bytes.new(1000) { |i| (i % 251).to_u8 }
      received, _ = forward_through_follower(LB_V1_HEADER.to_slice + data)
      String.new(received[0, LB_V1_HEADER.bytesize]).should eq LB_V1_HEADER
      received[LB_V1_HEADER.bytesize..].should eq data
    end

    it "adds a header to a connection without one" do
      header, _ = forward_through_follower(Bytes.empty)
      String.new(header).match(OWN_V1_HEADER).should_not be_nil
    end
  end

  describe "terminating TLS" do
    it "sends a v2 header with the TLS session to the leader's plain port" do
      header, client_address = forward_through_follower(Bytes.empty, tls: true) do |config|
        config.tcp_proxy_protocol = false
      end
      conn_info = parse_header(header).to_conn_info.should_not be_nil
      conn_info.remote_address.port.should eq client_address.port
      conn_info.ssl?.should be_true
      conn_info.ssl_verify?.should be_false
      conn_info.ssl_version.should_not be_nil
      conn_info.ssl_version.to_s.should start_with "TLSv1."
      conn_info.ssl_cipher.should_not be_nil
    end

    it "reads a load balancer's header in front of TLS, and adds the TLS session to it" do
      header, _ = forward_through_follower(LB_V1_HEADER.to_slice, tls: true)
      conn_info = parse_header(header).to_conn_info.should_not be_nil
      conn_info.remote_address.to_s.should eq "203.0.113.7:40000"
      conn_info.ssl?.should be_true
      conn_info.ssl_version.to_s.should start_with "TLSv1."
    end

    it "keeps the other TLVs of a load balancer's v2 header, and replaces its SSL TLV" do
      header, _ = forward_through_follower(proxy_v2_header_with_tlvs, tls: true)
      v2 = parse_header(header).as(LavinMQ::ProxyProtocol::V2)
      v2.tlvs.count(&.ssl?).should eq 1
      v2.tlvs.find! { |t| t.type == 0x01u8 }.value.should eq "amqp".to_slice
      conn_info = v2.to_conn_info.should_not be_nil
      conn_info.remote_address.to_s.should eq "203.0.113.7:40000"
      conn_info.ssl_cipher.should_not eq "lb-cipher"
    end
  end

  it "hands HTTP to the broker with the client's address" do
    with_amqp_server do |s|
      config = listener_config
      config.amqp_port = config.amqps_port = -1
      config.http_bind = "127.0.0.1"
      config.http_port = free_port
      listeners = LavinMQ::ClientListeners.new(config, -> { nil.as(Leader?) })
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
    # Knows a leader, so that it accepts
    listeners = NodelayListeners.new(config, -> { leader_at("127.0.0.1", {} of String => Int32) })
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
      listeners = LavinMQ::ClientListeners.new(config, -> { nil.as(Leader?) })
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
    listeners = LavinMQ::ClientListeners.new(config, -> { leader_at("127.0.0.1", {"amqp" => leader.local_address.port}) })
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

  it "lets connections wait while neither serving nor knowing of a leader" do
    with_amqp_server do |s|
      config = listener_config
      config.amqps_port = -1
      leader = TCPServer.new("127.0.0.1", 0)
      leader_host : String? = nil
      listeners = LavinMQ::ClientListeners.new(config, -> {
        if host = leader_host
          leader_at(host, {"amqp" => leader.local_address.port})
        end.as(Leader?)
      })
      listeners.bind(nil, nil, nil)
      listeners.listen
      early = TCPSocket.new("127.0.0.1", config.amqp_port)
      early.read_timeout = 100.milliseconds
      expect_raises(IO::TimeoutError) { early.read_byte } # not closed

      leader_host = "127.0.0.1"
      listeners.leader_changed
      leader.accept.close # forwarded once there's a leader

      # The leader is gone, e.g. this node won the election and loads its data
      leader_host = nil
      listeners.leader_changed
      promoted = TCPSocket.new("127.0.0.1", config.amqp_port)
      promoted.read_timeout = 100.milliseconds
      expect_raises(IO::TimeoutError) { promoted.read_byte }

      listeners.serve(LavinMQ::ClientListeners::Brokers.new(s.amqp_server, s.mqtt_server, s.http_server))
      promoted.read_timeout = 5.seconds
      promoted.write "AMQP\u0000\u0000\u0009\u0001".to_slice
      promoted.read_byte.should eq 1 # a method frame, Connection.Start
    ensure
      early.try &.close
      promoted.try &.close
      listeners.try &.close
      leader.try &.close
    end
  end
end
