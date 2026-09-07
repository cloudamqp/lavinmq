require "./spec_helper"

# End-to-end test of the SASL EXTERNAL mechanism over mTLS. amqp-client only
# speaks PLAIN, so the AMQP handshake is driven by hand.
describe "EXTERNAL authentication over mTLS" do
  it "authenticates an AMQP connection using the client certificate CN" do
    with_external_auth_amqp_server do |port, s|
      LavinMQ::Config.instance.external_auth_login_from = "common_name"
      user = s.@users.create("anders", "irrelevant-password")
      s.@users.add_permission(user, "/", /.*/, /.*/, /.*/)
      external_amqp_handshake_succeeds?(port).should be_true
    end
  end

  it "authenticates using a subject alternative name selected by type and index" do
    with_external_auth_amqp_server do |port, s|
      # The client certificate carries DNS SANs "anders", "anders", "localhost";
      # DNS index 2 resolves to "localhost".
      LavinMQ::Config.instance.external_auth_login_from = "subject_alternative_name"
      LavinMQ::Config.instance.external_auth_san_type = "DNS"
      LavinMQ::Config.instance.external_auth_san_index = 2
      user = s.@users.create("localhost", "irrelevant-password")
      s.@users.add_permission(user, "/", /.*/, /.*/, /.*/)
      external_amqp_handshake_succeeds?(port).should be_true
    end
  end
end

# Starts an mTLS-enabled AMQP server that requires a client certificate and
# yields the bound port and the server.
def with_external_auth_amqp_server(&)
  config = LavinMQ::Config.new
  LavinMQ::Config.instance = init_config(config)

  server_ctx = OpenSSL::SSL::Context::Server.new
  server_ctx.certificate_chain = "spec/resources/server_certificate.pem"
  server_ctx.private_key = "spec/resources/server_key.pem"
  server_ctx.ca_certificates = "spec/resources/ca_certificate.pem"
  server_ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT

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

# Drives a manual AMQP 0-9-1 handshake using SASL EXTERNAL over mTLS.
# Returns true if the server replies with Connection::OpenOk.
def external_amqp_handshake_succeeds?(port) : Bool
  tcp = TCPSocket.new("127.0.0.1", port)
  client_ctx = OpenSSL::SSL::Context::Client.new
  client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
  client_ctx.private_key = "spec/resources/client_key.pem"
  client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx, hostname: "localhost", sync_close: true)
  begin
    stream = AMQ::Protocol::Stream.new(ssl)
    stream.write AMQ::Protocol::PROTOCOL_START_0_9_1.to_slice
    stream.flush
    return false unless stream.next_frame.is_a?(AMQ::Protocol::Frame::Connection::Start)

    props = AMQ::Protocol::Table.new({
      "capabilities" => AMQ::Protocol::Table.new({"authentication_failure_close" => true}),
    })
    stream.write_bytes AMQ::Protocol::Frame::Connection::StartOk.new(props, "EXTERNAL", "", "en_US"),
      IO::ByteFormat::NetworkEndian
    stream.flush

    tune = stream.next_frame
    return false unless tune.is_a?(AMQ::Protocol::Frame::Connection::Tune)
    stream.write_bytes AMQ::Protocol::Frame::Connection::TuneOk.new(tune.channel_max, tune.frame_max, 0_u16),
      IO::ByteFormat::NetworkEndian
    stream.write_bytes AMQ::Protocol::Frame::Connection::Open.new("/"), IO::ByteFormat::NetworkEndian
    stream.flush

    stream.next_frame.is_a?(AMQ::Protocol::Frame::Connection::OpenOk)
  rescue
    false
  ensure
    ssl.close rescue nil
  end
end
