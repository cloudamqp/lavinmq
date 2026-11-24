require "./spec_helper"

describe "EXTERNAL authentication over mTLS" do
  it "authenticates an AMQP connection using the client certificate CN" do
    with_external_auth_amqp_server do |port, s|
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      user = s.@users.create("anders", "irrelevant-password")
      s.@users.add_permission(user, "/", /.*/, /.*/, /.*/)
      external_amqp_handshake(port).should be_a AMQ::Protocol::Frame::Connection::OpenOk
    end
  end

  it "authenticates using a subject alternative name selected by type and index" do
    with_external_auth_amqp_server do |port, s|
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      LavinMQ::Config.instance.external_auth_san_index = 2
      user = s.@users.create("localhost", "irrelevant-password")
      s.@users.add_permission(user, "/", /.*/, /.*/, /.*/)
      external_amqp_handshake(port).should be_a AMQ::Protocol::Frame::Connection::OpenOk
    end
  end

  it "advertises EXTERNAL when enabled and the client certificate is verified" do
    with_external_auth_amqp_server do |port, _s|
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      advertised_mechanisms(port).split(' ').should contain "EXTERNAL"
    end
  end

  it "doesn't advertise EXTERNAL when not enabled" do
    with_external_auth_amqp_server do |port, _s|
      advertised_mechanisms(port).split(' ').should_not contain "EXTERNAL"
    end
  end

  it "doesn't advertise EXTERNAL without a client certificate" do
    with_external_auth_amqp_server(require_client_cert: false) do |port, _s|
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      advertised_mechanisms(port, client_cert: false).split(' ').should_not contain "EXTERNAL"
    end
  end

  it "refuses EXTERNAL without a client certificate" do
    with_external_auth_amqp_server(require_client_cert: false) do |port, s|
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      s.@users.create("anders", "irrelevant-password")
      frame = external_amqp_handshake(port, client_cert: false)
      frame = frame.should be_a AMQ::Protocol::Frame::Connection::Close
      frame.reply_code.should eq 403
    end
  end

  it "refuses EXTERNAL when not enabled" do
    with_external_auth_amqp_server do |port, s|
      s.@users.create("anders", "irrelevant-password")
      frame = external_amqp_handshake(port)
      frame = frame.should be_a AMQ::Protocol::Frame::Connection::Close
      frame.reply_code.should eq 403
    end
  end

  it "refuses EXTERNAL when no user matches the certificate" do
    with_external_auth_amqp_server do |port, _s|
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      frame = external_amqp_handshake(port)
      frame = frame.should be_a AMQ::Protocol::Frame::Connection::Close
      frame.reply_code.should eq 403
    end
  end

  describe "behind a PROXY protocol header" do
    it "authenticates when LavinMQ terminates TLS, as for a connection a follower proxies" do
      with_external_auth_amqp_server do |port, s|
        LavinMQ::Config.instance.external_auth_login_from = "common_name"
        user = s.@users.create("anders", "irrelevant-password")
        s.@users.add_permission(user, "/", /.*/, /.*/, /.*/)
        header = LavinMQ::ProxyProtocol::V1.new(
          Socket::IPAddress.new("203.0.113.7", 40000), Socket::IPAddress.new("127.0.0.1", port))
        frame = external_amqp_handshake(port, proxy_header: header)
        frame.should be_a AMQ::Protocol::Frame::Connection::OpenOk
      end
    end

    it "ignores a client certificate CN sent in the header" do
      with_external_auth_amqp_server(require_client_cert: false) do |port, s|
        LavinMQ::Config.instance.external_auth_login_from = "common_name"
        s.@users.create("anders", "irrelevant-password")
        ssl_tlv = Bytes[0x03, 0, 0, 0, 0, 0x22, 0x00, 0x06] + "anders".to_slice
        header = LavinMQ::ProxyProtocol::V2.new(
          Socket::IPAddress.new("203.0.113.7", 40000), Socket::IPAddress.new("127.0.0.1", port),
          [LavinMQ::ProxyProtocol::V2::TLV.new(0x20_u8, ssl_tlv)])
        frame = external_amqp_handshake(port, client_cert: false, proxy_header: header)
        frame = frame.should be_a AMQ::Protocol::Frame::Connection::Close
        frame.reply_code.should eq 403
      end
    end
  end
end

def with_external_auth_amqp_server(require_client_cert = true, &)
  config = LavinMQ::Config.new
  config.tcp_proxy_protocol = true
  config.proxy_protocol_trusted_sources = LavinMQ::IPMatcher.parse_list("127.0.0.1")
  LavinMQ::Config.instance = init_config(config)

  server_ctx = OpenSSL::SSL::Context::Server.new
  server_ctx.certificate_chain = "spec/resources/server_certificate.pem"
  server_ctx.private_key = "spec/resources/server_key.pem"
  server_ctx.ca_certificates = "spec/resources/ca_certificate.pem"
  server_ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER
  server_ctx.verify_mode |= OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT if require_client_cert

  tcp_server = TCPServer.new("127.0.0.1", 0)
  port = tcp_server.local_address.port

  s = LavinMQ::Server.new(config, nil)
  begin
    amqp_server = s.amqp_server
    amqp_server.bind_tls(tcp_server, server_ctx)
    spawn(name: "external auth amqp listen") { amqp_server.listen }
    Fiber.yield
    yield port, s
  ensure
    s.close
  end
end

def with_external_auth_client_stream(port, client_cert = true, proxy_header = nil, &)
  tcp = TCPSocket.new("127.0.0.1", port)
  if proxy_header
    tcp.write_bytes proxy_header, IO::ByteFormat::NetworkEndian
    tcp.flush
  end
  client_ctx = OpenSSL::SSL::Context::Client.new
  if client_cert
    client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
    client_ctx.private_key = "spec/resources/client_key.pem"
  end
  client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx, hostname: "localhost", sync_close: true)
  begin
    stream = AMQ::Protocol::Stream.new(ssl)
    stream.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
    stream.flush
    start = stream.next_frame.as(AMQ::Protocol::Frame::Connection::Start)
    yield stream, start
  ensure
    ssl.close rescue nil
  end
end

def advertised_mechanisms(port, client_cert = true) : String
  with_external_auth_client_stream(port, client_cert) { |_stream, start| start.@mechanisms }
end

def external_amqp_handshake(port, client_cert = true, proxy_header = nil) : AMQ::Protocol::Frame
  with_external_auth_client_stream(port, client_cert, proxy_header) do |stream, _start|
    props = AMQ::Protocol::Table.new({
      "capabilities" => AMQ::Protocol::Table.new({"authentication_failure_close" => true}),
    })
    stream.write_bytes AMQ::Protocol::Frame::Connection::StartOk.new(props, "EXTERNAL", "", "en_US"),
      IO::ByteFormat::NetworkEndian
    stream.flush

    tune = stream.next_frame
    next tune unless tune.is_a?(AMQ::Protocol::Frame::Connection::Tune)
    stream.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(tune.channel_max, tune.frame_max, 0_u16),
      IO::ByteFormat::NetworkEndian
    stream.write_bytes AMQ::Protocol::Frame::Connection::Open.new("/"), IO::ByteFormat::NetworkEndian
    stream.flush
    stream.next_frame
  end
end
