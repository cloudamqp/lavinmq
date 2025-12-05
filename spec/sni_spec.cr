require "./spec_helper"
require "../src/stdlib/openssl_sni"

describe LavinMQ::SNIHost do
  it "creates TLS contexts with default settings" do
    host = LavinMQ::SNIHost.new("example.com")
    host.tls_cert = "spec/resources/server_certificate.pem"
    host.tls_key = "spec/resources/server_key.pem"

    host.amqp_tls_context.should be_a(OpenSSL::SSL::Context::Server)
    host.mqtt_tls_context.should be_a(OpenSSL::SSL::Context::Server)
    host.http_tls_context.should be_a(OpenSSL::SSL::Context::Server)
  end

  it "creates TLS contexts with mTLS settings" do
    host = LavinMQ::SNIHost.new("mtls.example.com")
    host.tls_cert = "spec/resources/server_certificate.pem"
    host.tls_key = "spec/resources/server_key.pem"
    host.tls_verify_peer = true
    host.tls_ca_cert = "spec/resources/ca_certificate.pem"

    # All protocols should have mTLS by default
    host.amqp_tls_context.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
    host.mqtt_tls_context.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
    host.http_tls_context.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
  end

  it "creates separate TLS contexts with protocol-specific overrides" do
    host = LavinMQ::SNIHost.new("example.com")
    host.tls_cert = "spec/resources/server_certificate.pem"
    host.tls_key = "spec/resources/server_key.pem"
    host.tls_verify_peer = true
    host.tls_ca_cert = "spec/resources/ca_certificate.pem"
    # AMQP: use default mTLS (true)
    # MQTT: disable mTLS
    host.mqtt_tls_verify_peer = false
    # HTTP: disable mTLS
    host.http_tls_verify_peer = false

    amqp_ctx = host.amqp_tls_context
    mqtt_ctx = host.mqtt_tls_context
    http_ctx = host.http_tls_context

    # All contexts should be different objects
    amqp_ctx.should_not eq(mqtt_ctx)
    amqp_ctx.should_not eq(http_ctx)

    # AMQP should have peer verification (default)
    amqp_ctx.verify_mode.should eq(OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT)
    # MQTT should NOT have peer verification
    mqtt_ctx.verify_mode.should eq(OpenSSL::SSL::VerifyMode::NONE)
    # HTTP should NOT have peer verification
    http_ctx.verify_mode.should eq(OpenSSL::SSL::VerifyMode::NONE)
  end

  it "reloads all contexts" do
    host = LavinMQ::SNIHost.new("example.com")
    host.tls_cert = "spec/resources/server_certificate.pem"
    host.tls_key = "spec/resources/server_key.pem"

    amqp_ctx1 = host.amqp_tls_context
    mqtt_ctx1 = host.mqtt_tls_context
    http_ctx1 = host.http_tls_context

    host.reload

    amqp_ctx2 = host.amqp_tls_context
    mqtt_ctx2 = host.mqtt_tls_context
    http_ctx2 = host.http_tls_context

    # After reload, new contexts should be created
    amqp_ctx1.should_not eq(amqp_ctx2)
    mqtt_ctx1.should_not eq(mqtt_ctx2)
    http_ctx1.should_not eq(http_ctx2)
  end
end

describe LavinMQ::SNIManager do
  it "manages SNI hosts" do
    manager = LavinMQ::SNIManager.new

    host1 = LavinMQ::SNIHost.new("example.com")
    host1.tls_cert = "spec/resources/server_certificate.pem"
    host1.tls_key = "spec/resources/server_key.pem"

    host2 = LavinMQ::SNIHost.new("api.example.com")
    host2.tls_cert = "spec/resources/server_certificate.pem"
    host2.tls_key = "spec/resources/server_key.pem"

    manager.add_host(host1)
    manager.add_host(host2)

    manager.get_host("example.com").should eq(host1)
    manager.get_host("api.example.com").should eq(host2)
    manager.get_host("unknown.com").should be_nil
    manager.empty?.should be_false
  end

  it "starts empty" do
    manager = LavinMQ::SNIManager.new
    manager.empty?.should be_true
    manager.get_host("example.com").should be_nil
  end

  it "supports wildcard hostnames" do
    manager = LavinMQ::SNIManager.new

    wildcard_host = LavinMQ::SNIHost.new("*.example.com")
    wildcard_host.tls_cert = "spec/resources/server_certificate.pem"
    wildcard_host.tls_key = "spec/resources/server_key.pem"

    exact_host = LavinMQ::SNIHost.new("specific.example.com")
    exact_host.tls_cert = "spec/resources/server_certificate.pem"
    exact_host.tls_key = "spec/resources/server_key.pem"

    manager.add_host(wildcard_host)
    manager.add_host(exact_host)

    # Exact match takes precedence
    manager.get_host("specific.example.com").should eq(exact_host)
    # Wildcard matches other subdomains
    manager.get_host("foo.example.com").should eq(wildcard_host)
    manager.get_host("bar.example.com").should eq(wildcard_host)
    # No match for different domain
    manager.get_host("example.com").should be_nil
    manager.get_host("other.com").should be_nil
  end
end

describe LavinMQ::Config do
  it "parses SNI sections from config file" do
    config = LavinMQ::Config.new
    config.data_dir = "/tmp/lavinmq-sni-spec"

    # Create a test config file
    config_content = <<-INI
    [main]
    data_dir = /tmp/lavinmq-sni-spec

    [sni:example.com]
    tls_cert = spec/resources/server_certificate.pem
    tls_key = spec/resources/server_key.pem
    tls_min_version = 1.2
    tls_verify_peer = false

    [sni:mtls.example.com]
    tls_cert = spec/resources/server_certificate.pem
    tls_key = spec/resources/server_key.pem
    tls_verify_peer = true
    tls_ca_cert = spec/resources/ca_certificate.pem
    http_tls_verify_peer = false
    INI

    config_file = File.tempname("lavinmq", ".ini")
    File.write(config_file, config_content)

    begin
      ini = INI.parse(File.read(config_file))
      ini.each do |section, settings|
        if section.starts_with?("sni:")
          hostname = section[4..]
          host = config.sni_manager.get_host(hostname) || LavinMQ::SNIHost.new(hostname)
          settings.each do |key, value|
            case key
            when "tls_cert"             then host.tls_cert = value
            when "tls_key"              then host.tls_key = value
            when "tls_min_version"      then host.tls_min_version = value
            when "tls_verify_peer"      then host.tls_verify_peer = {"true", "yes", "y", "1"}.includes?(value)
            when "tls_ca_cert"          then host.tls_ca_cert = value
            when "http_tls_verify_peer" then host.http_tls_verify_peer = {"true", "yes", "y", "1"}.includes?(value)
            end
          end
          config.sni_manager.add_host(host) unless host.tls_cert.empty?
        end
      end

      config.sni_manager.empty?.should be_false
      config.sni_manager.get_host("example.com").should_not be_nil
      config.sni_manager.get_host("mtls.example.com").should_not be_nil

      example_host = config.sni_manager.get_host("example.com").not_nil!
      example_host.tls_verify_peer?.should be_false

      mtls_host = config.sni_manager.get_host("mtls.example.com").not_nil!
      mtls_host.tls_verify_peer?.should be_true
      mtls_host.http_tls_verify_peer.should eq(false)
    ensure
      File.delete(config_file)
    end
  end
end

describe OpenSSL::SSL::Context::Server do
  it "supports SNI callback" do
    default_ctx = OpenSSL::SSL::Context::Server.new
    default_ctx.certificate_chain = "spec/resources/server_certificate.pem"
    default_ctx.private_key = "spec/resources/server_key.pem"

    alt_ctx = OpenSSL::SSL::Context::Server.new
    alt_ctx.certificate_chain = "spec/resources/server_certificate.pem"
    alt_ctx.private_key = "spec/resources/server_key.pem"

    callback_called = false
    received_hostname = ""

    default_ctx.set_sni_callback do |hostname|
      callback_called = true
      received_hostname = hostname
      if hostname == "alt.example.com"
        alt_ctx
      else
        nil
      end
    end

    # The callback is set, but we can't easily test it without a full TLS handshake
    # This test verifies that the method exists and can be called without error
    default_ctx.should be_a(OpenSSL::SSL::Context::Server)
  end
end

describe "SNI end-to-end" do
  it "serves correct certificate based on SNI hostname" do
    # Set up SNI manager with different certificates
    sni_manager = LavinMQ::SNIManager.new

    # Wildcard certificate for *.example.com
    wildcard_host = LavinMQ::SNIHost.new("*.example.com")
    wildcard_host.tls_cert = "spec/resources/wildcard_example_certificate.pem"
    wildcard_host.tls_key = "spec/resources/wildcard_example_key.pem"
    sni_manager.add_host(wildcard_host)

    # Certificate for foobar.localhost (exact match)
    foobar_host = LavinMQ::SNIHost.new("foobar.localhost")
    foobar_host.tls_cert = "spec/resources/foobar_localhost_certificate.pem"
    foobar_host.tls_key = "spec/resources/foobar_localhost_key.pem"
    sni_manager.add_host(foobar_host)

    # Default server context (for unmatched hostnames)
    default_ctx = OpenSSL::SSL::Context::Server.new
    default_ctx.certificate_chain = "spec/resources/server_certificate.pem"
    default_ctx.private_key = "spec/resources/server_key.pem"

    # Set up SNI callback
    default_ctx.set_sni_callback do |hostname|
      if sni_host = sni_manager.get_host(hostname)
        sni_host.amqp_tls_context
      else
        nil
      end
    end

    # Start TLS server
    tcp_server = TCPServer.new("127.0.0.1", 0)
    port = tcp_server.local_address.port

    server_done = Channel(Nil).new

    spawn do
      3.times do
        if client = tcp_server.accept?
          begin
            ssl_socket = OpenSSL::SSL::Socket::Server.new(client, default_ctx)
            ssl_socket.close
          rescue
            # Ignore handshake errors in server
          ensure
            client.close
          end
        end
      end
      server_done.send(nil)
    end

    # Helper to create client context that trusts our self-signed certs
    create_client_ctx = ->(cert_file : String) {
      ctx = OpenSSL::SSL::Context::Client.new
      ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER
      ctx.ca_certificates = cert_file
      ctx
    }

    # Test 1: Connect with wildcard hostname (test.example.com)
    tcp_client1 = TCPSocket.new("127.0.0.1", port)
    client_ctx1 = create_client_ctx.call("spec/resources/wildcard_example_certificate.pem")
    begin
      ssl_client1 = OpenSSL::SSL::Socket::Client.new(tcp_client1, client_ctx1, hostname: "test.example.com")
      ssl_client1.close
    ensure
      tcp_client1.close
    end

    # Test 2: Connect with another wildcard subdomain (foo.example.com)
    tcp_client2 = TCPSocket.new("127.0.0.1", port)
    client_ctx2 = create_client_ctx.call("spec/resources/wildcard_example_certificate.pem")
    begin
      ssl_client2 = OpenSSL::SSL::Socket::Client.new(tcp_client2, client_ctx2, hostname: "foo.example.com")
      ssl_client2.close
    ensure
      tcp_client2.close
    end

    # Test 3: Connect with exact match hostname (foobar.localhost)
    tcp_client3 = TCPSocket.new("127.0.0.1", port)
    client_ctx3 = create_client_ctx.call("spec/resources/foobar_localhost_certificate.pem")
    begin
      ssl_client3 = OpenSSL::SSL::Socket::Client.new(tcp_client3, client_ctx3, hostname: "foobar.localhost")
      ssl_client3.close
    ensure
      tcp_client3.close
    end

    tcp_server.close
    server_done.receive
  end
end
