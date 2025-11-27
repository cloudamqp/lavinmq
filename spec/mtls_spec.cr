require "./spec_helper"
require "http/client"

describe LavinMQ::Server do
  describe "mTLS (mutual TLS)" do
    it "should accept connections with valid client certificates" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = true

      with_amqp_server(tls: true, config: config) do |s|
        # Create client context WITHOUT client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should fail to connect without client certificate
        expect_raises(AMQP::Client::Error, /certificate required/) do
          AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        end

        # Add valid client certificates to client context
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"

        # Should successfully connect with valid client certificate
        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        conn.should_not be_nil
        channel = conn.channel
        channel.should_not be_nil
        conn.close
      end
    end

    it "should reject connections without client certificates when required" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = true

      with_amqp_server(tls: true, config: config) do |s|
        # Create client context WITHOUT client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should fail to connect without client certificate
        expect_raises(AMQP::Client::Error, /certificate required/) do
          AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        end
      end
    end

    it "should accept connections without client certificates when not required" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = false

      with_amqp_server(tls: true, config: config) do |s|
        # Create client context WITHOUT client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should successfully connect even without client certificate
        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        conn.should_not be_nil
        channel = conn.channel
        channel.should_not be_nil
        conn.close
      end
    end

    it "should populate ConnectionInfo with client certificate details" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = true

      with_amqp_server(tls: true, config: config) do |s|
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect

        # Wait for server to process connection
        wait_for { s.vhosts["/"].connections.size > 0 }

        # Check that connection info was populated with certificate details
        connections = s.vhosts["/"].connections.to_a
        connections.size.should eq 1
        conn_info = connections.first.connection_info

        conn_info.ssl?.should be_true
        conn_info.ssl_verify?.should be_true
        conn_info.ssl_cn.should eq "anders"
        conn_info.ssl_sig_alg.should eq "SHA256"

        conn.close
      end
    end

    it "should work with MQTT over mTLS" do
      with_mqtts_server(verify_peer: true, require_peer_cert: true) do |_s, port|
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        socket = TCPSocket.new("localhost", port)
        ssl_socket = OpenSSL::SSL::Socket::Client.new(socket, context: client_ctx, sync_close: true, hostname: "localhost")

        # Send MQTT CONNECT packet
        connect = IO::Memory.new
        connect.write_byte(0x10_u8) # CONNECT packet type
        connect.write_byte(0x10_u8) # Remaining length
        connect.write_bytes(0x00_u16, IO::ByteFormat::BigEndian)
        connect.write("MQTT".to_slice)
        connect.write_byte(0x04_u8)                                # Protocol level
        connect.write_byte(0x02_u8)                                # Connect flags (clean session)
        connect.write_bytes(0x003c_u16, IO::ByteFormat::BigEndian) # Keep alive
        connect.write_bytes(0x0004_u16, IO::ByteFormat::BigEndian) # Client ID length
        connect.write("test".to_slice)

        ssl_socket.write(connect.to_slice)
        ssl_socket.flush

        # Read CONNACK
        packet_type = ssl_socket.read_byte
        packet_type.should eq 0x20_u8 # CONNACK

        ssl_socket.close
      end
    end
  end

  describe "CRL (Certificate Revocation List)" do
    it "should accept connections with valid (non-revoked) certificates when CRL is enabled" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = true
      config.tls_crl_file = "spec/resources/empty_crl.pem"

      with_amqp_server(tls: true, config: config) do |s|
        # Create client context with valid (non-revoked) client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should successfully connect with valid non-revoked certificate
        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        conn.should_not be_nil
        channel = conn.channel
        channel.should_not be_nil
        conn.close
      end
    end

    it "should reject connections with revoked certificates when CRL is enabled" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = true
      config.tls_crl_file = "spec/resources/crl.pem"

      with_amqp_server(tls: true, config: config) do |s|
        # Create client context with revoked client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/revoked_client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should fail to connect with revoked certificate
        expect_raises(AMQP::Client::Error, /certificate revoked/) do
          AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        end
      end
    end

    it "should accept valid certificates when CRL checking is not enabled" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_certificate.pem"
      config.tls_key_path = "spec/resources/server_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_certificate.pem"
      config.tls_fail_if_no_peer_cert = true

      with_amqp_server(tls: true, config: config) do |s|
        # Even with a revoked certificate, if CRL checking is not enabled, it should work
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/revoked_client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should successfully connect because CRL checking is disabled
        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        conn.should_not be_nil
        conn.close
      end
    end
  end

  describe "CDP (CRL Distribution Point)" do
    it "should extract CDP URLs from CA certificates" do
      # Test that CDP URL extraction works
      urls = OpenSSL::X509.extract_crl_urls_from_cert("spec/resources/ca_with_cdp_certificate.pem")
      urls.should_not be_empty
      urls.first.should eq "http://localhost:30080/crl.pem"
    end

    it "should handle CA certificates without CDP extensions gracefully" do
      # Test that extraction works on certificates without CDP
      urls = OpenSSL::X509.extract_crl_urls_from_cert("spec/resources/ca_certificate.pem")
      urls.should be_empty
    end

    it "should fail to start when remote server is unreachable and no cache is available" do
      # CA cert has CDP extension pointing to http://localhost:30080/crl.pem
      # but no server is running on that port, so CRL fetch should fail
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq_cdp_unreachable_spec"
      config.tls_cert_path = "spec/resources/server_with_cdp_certificate.pem"
      config.tls_key_path = "spec/resources/server_with_cdp_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_with_cdp_certificate.pem"
      config.tls_fail_if_no_peer_cert = true

      begin
        # Should fail to start because CDP URL is unreachable and no cache exists
        expect_raises(Socket::ConnectError) do
          with_amqp_server(tls: true, config: config) do |_s|
            # Should not reach here
          end
        end
      ensure
        FileUtils.rm_rf("/tmp/lavinmq_cdp_unreachable_spec")
      end
    end

    it "should use cached CRL when remote server is unreachable" do
      config = LavinMQ::Config.new
      config.data_dir = "/tmp/lavinmq-spec"
      config.tls_cert_path = "spec/resources/server_with_cdp_certificate.pem"
      config.tls_key_path = "spec/resources/server_with_cdp_key.pem"
      config.tls_verify_peer = true
      config.tls_ca_cert_path = "spec/resources/ca_with_cdp_certificate.pem"
      config.tls_fail_if_no_peer_cert = true

      # Pre-populate cache with a CRL
      cache_dir = "/tmp/lavinmq-spec/crl_cache"
      Dir.mkdir_p(cache_dir)

      url = "http://localhost:30080/crl.pem"
      url_hash = Digest::SHA1.hexdigest(url)
      FileUtils.cp("spec/resources/empty_crl.pem", File.join(cache_dir, "#{url_hash}.crl"))

      # Create a TLS context and try to load CRL from unreachable URL
      # It should fall back to the cached version
      with_amqp_server(tls: true, config: config) do |s|
        # If we get here, the cached CRL was used successfully
        s.should_not be_nil
      end
    end

    it "should dynamically update CRL and reject revoked certificates" do
      # Start HTTP server on port 30080 that initially serves empty CRL, then serves revoked CRL
      empty_crl = File.read("spec/resources/empty_cdp_crl.pem")
      revoked_crl = File.read("spec/resources/revoked_crl.pem")
      serve_revoked = false

      server = HTTP::Server.new do |context|
        if context.request.path == "/crl.pem"
          context.response.content_type = "application/pkix-crl"
          context.response.print(serve_revoked ? revoked_crl : empty_crl)
        else
          context.response.status_code = 404
        end
      end

      server.bind_tcp("127.0.0.1", 30080)
      spawn { server.listen }

      begin
        # Configure server with CDP-enabled CA certificate
        # CA cert has CDP extension pointing to http://localhost:30080/crl.pem
        # Server automatically extracts CDP URL and tracks it for CRL updates
        config = LavinMQ::Config.new
        config.data_dir = "/tmp/lavinmq_cdp_spec"
        config.tls_cert_path = "spec/resources/server_with_cdp_certificate.pem"
        config.tls_key_path = "spec/resources/server_with_cdp_key.pem"
        config.tls_verify_peer = true
        config.tls_ca_cert_path = "spec/resources/ca_with_cdp_certificate.pem"
        config.tls_fail_if_no_peer_cert = true

        with_amqp_server(tls: true, config: config) do |s|
          # First, connect with client certificate (not revoked yet)
          client_ctx = OpenSSL::SSL::Context::Client.new
          client_ctx.certificate_chain = "spec/resources/client_with_cdp_certificate.pem"
          client_ctx.private_key = "spec/resources/client_with_cdp_key.pem"
          client_ctx.ca_certificates = "spec/resources/ca_with_cdp_certificate.pem"

          # Should successfully connect because CRL is empty (no revocations)
          conn1 = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
          conn1.should_not be_nil
          channel1 = conn1.channel
          channel1.should_not be_nil
          conn1.close

          # Update server to serve revoked CRL
          serve_revoked = true

          # Trigger CRL update by fetching from CDP URL embedded in CA certificate
          s.update_cdp_crls

          sleep 10.milliseconds # Wait a bit for CRL to be updated

          # Try to connect again with the same certificate (now revoked)
          # Should fail because the X509_STORE was updated with the revoked CRL
          expect_raises(AMQP::Client::Error, /certificate revoked/) do
            AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
          end
        end
      ensure
        server.close
        FileUtils.rm_rf("/tmp/lavinmq_cdp_spec")
      end
    end
  end
end
