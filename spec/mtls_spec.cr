require "./spec_helper"

describe "mTLS (Mutual TLS)" do
  describe "client certificate authentication" do
    it "accepts connection with valid client certificate" do
      with_mtls_server do |port, _server|
        # Client presents valid certificate signed by trusted CA
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        # Don't verify server hostname for test certs (CN=anders, not localhost)
        client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

        tcp_client = TCPSocket.new("127.0.0.1", port)
        begin
          ssl_client = OpenSSL::SSL::Socket::Client.new(tcp_client, client_ctx, hostname: "localhost")
          ssl_client.tls_version.should_not be_nil
          ssl_client.close
        ensure
          tcp_client.close
        end
      end
    end

    it "rejects connection without client certificate" do
      with_mtls_server do |port, _server|
        # Client does not present any certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

        tcp_client = TCPSocket.new("127.0.0.1", port)
        begin
          # Server requires client cert, so handshake should fail
          # The error might be OpenSSL::SSL::Error or IO::Error depending on timing
          expect_raises(Exception) do
            ssl_client = OpenSSL::SSL::Socket::Client.new(tcp_client, client_ctx, hostname: "localhost")
            # If handshake succeeds, try to read which should fail
            ssl_client.gets
          end
        ensure
          tcp_client.close
        end
      end
    end

    it "accepts connection with any CA-signed client certificate" do
      # Both server and client certs are signed by the same CA, so server accepts both
      with_mtls_server do |port, _server|
        client_ctx = OpenSSL::SSL::Context::Client.new
        # Use server certificate as client cert (both signed by same CA)
        client_ctx.certificate_chain = "spec/resources/server_certificate.pem"
        client_ctx.private_key = "spec/resources/server_key.pem"
        client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

        tcp_client = TCPSocket.new("127.0.0.1", port)
        begin
          # Server should accept because this cert is signed by the trusted CA
          ssl_client = OpenSSL::SSL::Socket::Client.new(tcp_client, client_ctx, hostname: "localhost")
          ssl_client.tls_version.should_not be_nil
          ssl_client.close
        ensure
          tcp_client.close
        end
      end
    end
  end

  describe "AMQP with mTLS" do
    it "allows AMQP connection with valid client certificate" do
      with_mtls_amqp_server do |port, _server|
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"
        # Don't verify server hostname for test certs (CN=anders, not localhost)
        client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

        conn = AMQP::Client.new(
          host: "127.0.0.1",
          port: port,
          tls: client_ctx
        ).connect
        conn.channel.should be_a(AMQP::Client::Channel)
        conn.close
      end
    end

    it "rejects AMQP connection without client certificate" do
      with_mtls_amqp_server do |port, _server|
        client_ctx = OpenSSL::SSL::Context::Client.new
        # No client certificate, but disable client-side server verification for test
        client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

        expect_raises(AMQP::Client::Error, /SSL/) do
          AMQP::Client.new(
            host: "127.0.0.1",
            port: port,
            tls: client_ctx
          ).connect
        end
      end
    end

    it "can publish and consume messages over mTLS connection" do
      with_mtls_amqp_server do |port, _|
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"
        # Don't verify server hostname for test certs (CN=anders, not localhost)
        client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

        conn = AMQP::Client.new(
          host: "127.0.0.1",
          port: port,
          tls: client_ctx
        ).connect
        ch = conn.channel
        q = ch.queue("mtls-test-queue", auto_delete: true)

        q.publish("test message over mTLS")

        msg = q.get
        msg.should_not be_nil
        msg.not_nil!.body_io.gets_to_end.should eq("test message over mTLS")

        q.delete
        conn.close
      end
    end
  end

  describe "SNIHost mTLS configuration" do
    it "creates TLS context with mTLS enabled" do
      host = LavinMQ::SNIHost.new("mtls.example.com")
      host.tls_cert = "spec/resources/server_certificate.pem"
      host.tls_key = "spec/resources/server_key.pem"
      host.tls_verify_peer = true
      host.tls_ca_cert = "spec/resources/ca_certificate.pem"

      ctx = host.amqp_tls_context
      ctx.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
    end

    it "creates TLS context without mTLS when disabled" do
      host = LavinMQ::SNIHost.new("no-mtls.example.com")
      host.tls_cert = "spec/resources/server_certificate.pem"
      host.tls_key = "spec/resources/server_key.pem"
      host.tls_verify_peer = false

      ctx = host.amqp_tls_context
      ctx.verify_mode.should eq(OpenSSL::SSL::VerifyMode::NONE)
    end

    it "allows per-protocol mTLS configuration" do
      host = LavinMQ::SNIHost.new("mixed-mtls.example.com")
      host.tls_cert = "spec/resources/server_certificate.pem"
      host.tls_key = "spec/resources/server_key.pem"
      host.tls_verify_peer = true
      host.tls_ca_cert = "spec/resources/ca_certificate.pem"
      # Disable mTLS for HTTP only
      host.http_tls_verify_peer = false

      # AMQP and MQTT should require client certs
      host.amqp_tls_context.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
      host.mqtt_tls_context.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
      # HTTP should not
      host.http_tls_context.verify_mode.should eq(OpenSSL::SSL::VerifyMode::NONE)
    end
  end
end

# Helper to create an mTLS-enabled raw TLS server
def with_mtls_server(&)
  server_ctx = OpenSSL::SSL::Context::Server.new
  server_ctx.certificate_chain = "spec/resources/server_certificate.pem"
  server_ctx.private_key = "spec/resources/server_key.pem"
  server_ctx.ca_certificates = "spec/resources/ca_certificate.pem"
  server_ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT

  tcp_server = TCPServer.new("127.0.0.1", 0)
  port = tcp_server.local_address.port

  server_done = Channel(Nil).new
  spawn do
    loop do
      break unless client = tcp_server.accept?
      begin
        ssl_socket = OpenSSL::SSL::Socket::Server.new(client, server_ctx, sync_close: true)
        # Just accept and close for handshake test
        ssl_socket.close
      rescue
        # Ignore handshake errors
      ensure
        client.close rescue nil
      end
    end
    server_done.send(nil)
  end

  begin
    yield port, tcp_server
  ensure
    tcp_server.close
    select
    when server_done.receive
    when timeout(1.seconds)
    end
  end
end

# Helper to create an AMQP server with mTLS enabled
def with_mtls_amqp_server(file = __FILE__, line = __LINE__, &)
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
    spawn(name: "amqp mtls listen") { s.listen_tls(tcp_server, server_ctx, LavinMQ::Server::Protocol::AMQP) }
    Fiber.yield

    yield port, s
  ensure
    s.close
    FileUtils.rm_rf(config.data_dir)
    LavinMQ::Config.instance = init_config(LavinMQ::Config.new)
  end
end
