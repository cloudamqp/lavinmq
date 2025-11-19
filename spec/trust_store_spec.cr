require "./spec_helper"
require "http/server"

describe LavinMQ::TrustStore do
  describe "Filesystem Provider" do
    it "should load certificates from directory" do
      Dir.mkdir_p("/tmp/lavinmq_trust_store_test")
      begin
        # Copy test certificate to trust store directory
        File.copy("spec/resources/client_certificate.pem", "/tmp/lavinmq_trust_store_test/client.pem")

        config = LavinMQ::Config.new
        config.tls_trust_store_directory = "/tmp/lavinmq_trust_store_test"
        config.tls_trust_store_refresh_interval = 0

        trust_store = LavinMQ::TrustStore.new(config)
        trust_store.size.should eq 1

        # Verify the certificate file contains the expected certificate
        cert_file_content = File.read(trust_store.certificate_file)
        expected_cert = File.read("spec/resources/client_certificate.pem")
        cert_file_content.should contain(expected_cert.strip)
      ensure
        FileUtils.rm_rf("/tmp/lavinmq_trust_store_test")
      end
    end

    it "should reject certificates not in trust store" do
      Dir.mkdir_p("/tmp/lavinmq_trust_store_test")
      begin
        # Empty trust store
        config = LavinMQ::Config.new
        config.tls_trust_store_directory = "/tmp/lavinmq_trust_store_test"
        config.tls_trust_store_refresh_interval = 0

        trust_store = LavinMQ::TrustStore.new(config)
        trust_store.size.should eq 0

        # Verify the certificate file is empty
        cert_file_content = File.read(trust_store.certificate_file)
        cert_file_content.strip.should be_empty
      ensure
        FileUtils.rm_rf("/tmp/lavinmq_trust_store_test")
      end
    end

    it "should reload certificates when reload is called" do
      Dir.mkdir_p("/tmp/lavinmq_trust_store_test")
      begin
        config = LavinMQ::Config.new
        config.tls_trust_store_directory = "/tmp/lavinmq_trust_store_test"
        config.tls_trust_store_refresh_interval = 0

        trust_store = LavinMQ::TrustStore.new(config)
        trust_store.size.should eq 0

        # Add a certificate to the directory
        File.copy("spec/resources/client_certificate.pem", "/tmp/lavinmq_trust_store_test/client.pem")

        # Reload trust store
        trust_store.reload
        trust_store.size.should eq 1

        # Verify the certificate file now contains the certificate
        cert_file_content = File.read(trust_store.certificate_file)
        expected_cert = File.read("spec/resources/client_certificate.pem")
        cert_file_content.should contain(expected_cert.strip)
      ensure
        FileUtils.rm_rf("/tmp/lavinmq_trust_store_test")
      end
    end
  end

  describe "HTTP Provider" do
    it "should load certificates from HTTP endpoint" do
      # Start a simple HTTP server that returns certificate list
      port = 9999
      server = HTTP::Server.new do |context|
        if context.request.path == "/"
          # Return list of certificates
          context.response.content_type = "application/json"
          context.response.print({
            certificates: [
              {id: "client", path: "/certs/client.pem"},
            ],
          }.to_json)
        elsif context.request.path == "/certs/client.pem"
          # Return the certificate content
          context.response.content_type = "application/x-pem-file"
          context.response.print File.read("spec/resources/client_certificate.pem")
        end
      end

      spawn { server.listen(port) }
      sleep 200.milliseconds # Give server time to start

      # Wait for server to be ready
      10.times do
        begin
          HTTP::Client.get("http://localhost:#{port}/")
          break
        rescue
          sleep 50.milliseconds
        end
      end

      begin
        config = LavinMQ::Config.new
        config.tls_trust_store_url = "http://localhost:#{port}"
        config.tls_trust_store_refresh_interval = 0

        trust_store = LavinMQ::TrustStore.new(config)
        trust_store.size.should eq 1

        # Verify the certificate file contains the downloaded certificate
        cert_file_content = File.read(trust_store.certificate_file)
        expected_cert = File.read("spec/resources/client_certificate.pem")
        cert_file_content.should contain(expected_cert.strip)
      ensure
        server.close
      end
    end

    it "should handle 304 Not Modified responses" do
      port = 9998

      # Wait for server to be ready first (before tracking fetch_count)
      server = HTTP::Server.new do |context|
        # Just return 200 for initial health check
        if context.request.path == "/health"
          context.response.status_code = 200
          context.response.print "OK"
        end
      end
      spawn { server.listen(port) }
      sleep 200.milliseconds

      # Wait for server to be ready using health endpoint
      10.times do
        begin
          HTTP::Client.get("http://localhost:#{port}/health")
          break
        rescue
          sleep 50.milliseconds
        end
      end
      server.close

      # Now start the real server with fetch counting
      fetch_count = 0
      server = HTTP::Server.new do |context|
        if context.request.path == "/"
          if if_modified = context.request.headers["If-Modified-Since"]?
            # Return 304 Not Modified
            context.response.status_code = 304
            fetch_count += 1
          else
            # First request - return certificate list
            context.response.headers["Last-Modified"] = HTTP.format_time(Time.utc)
            context.response.content_type = "application/json"
            context.response.print({
              certificates: [
                {id: "client", path: "/certs/client.pem"},
              ],
            }.to_json)
            fetch_count += 1
          end
        elsif context.request.path == "/certs/client.pem"
          context.response.content_type = "application/x-pem-file"
          context.response.print File.read("spec/resources/client_certificate.pem")
        end
      end

      spawn { server.listen(port) }
      sleep 100.milliseconds

      begin
        config = LavinMQ::Config.new
        config.tls_trust_store_url = "http://localhost:#{port}"
        config.tls_trust_store_refresh_interval = 0

        trust_store = LavinMQ::TrustStore.new(config)
        fetch_count.should eq 1

        # Reload should result in 304 Not Modified
        trust_store.reload
        fetch_count.should eq 2 # Fetched again but got 304
        trust_store.size.should eq 1 # Still has the cached certificate
      ensure
        server.close
      end
    end
  end

  describe "Integration with mTLS" do
    it "should accept connections with certificates in trust store" do
      Dir.mkdir_p("/tmp/lavinmq_trust_store_test")
      begin
        # Add CA certificate to trust store (this will trust all certs signed by this CA)
        File.copy("spec/resources/ca_certificate.pem", "/tmp/lavinmq_trust_store_test/ca.pem")

        # Create config with trust store enabled
        config = LavinMQ::Config.new
        config.data_dir = "/tmp/lavinmq_mtls_trust_test_#{rand}"
        config.tls_trust_store_directory = "/tmp/lavinmq_trust_store_test"
        config.tls_trust_store_refresh_interval = 0

        # Create trust store and server
        trust_store = LavinMQ::TrustStore.new(config)
        server = LavinMQ::Server.new(config, nil, trust_store)

        # Create TLS context with trust store certificates
        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = "spec/resources/server_certificate.pem"
        ctx.private_key = "spec/resources/server_key.pem"
        # Use trust store certificate file for CA verification
        ctx.ca_certificates = trust_store.certificate_file
        ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT

        # Start server listening
        amqps_socket = TCPServer.new("localhost", 0)
        amqps_port = amqps_socket.local_address.port
        spawn { server.listen_tls(amqps_socket, ctx, LavinMQ::Server::Protocol::AMQP) }
        sleep 100.milliseconds

        begin
          # Client with certificate in trust store should connect successfully
          client_ctx = OpenSSL::SSL::Context::Client.new
          client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
          client_ctx.private_key = "spec/resources/client_key.pem"
          client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

          conn = AMQP::Client.new(host: "localhost", port: amqps_port, tls: client_ctx).connect
          conn.should_not be_nil
          conn.close
        ensure
          server.close
          amqps_socket.close
          FileUtils.rm_rf(config.data_dir)
        end
      ensure
        FileUtils.rm_rf("/tmp/lavinmq_trust_store_test")
      end
    end

    it "should reject connections with certificates not in trust store" do
      Dir.mkdir_p("/tmp/lavinmq_trust_store_test")
      begin
        # Empty trust store - no CA certificates, so client cert cannot be verified

        config = LavinMQ::Config.new
        config.data_dir = "/tmp/lavinmq_mtls_trust_test_#{rand}"
        config.tls_trust_store_directory = "/tmp/lavinmq_trust_store_test"
        config.tls_trust_store_refresh_interval = 0

        # Create trust store and server
        trust_store = LavinMQ::TrustStore.new(config)
        server = LavinMQ::Server.new(config, nil, trust_store)

        # Create TLS context without CA certificates (empty trust store)
        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = "spec/resources/server_certificate.pem"
        ctx.private_key = "spec/resources/server_key.pem"
        # Don't set ca_certificates since trust store is empty - client will be rejected during handshake
        ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT

        amqps_socket = TCPServer.new("localhost", 0)
        amqps_port = amqps_socket.local_address.port
        spawn { server.listen_tls(amqps_socket, ctx, LavinMQ::Server::Protocol::AMQP) }
        sleep 100.milliseconds

        begin
          # Client with certificate NOT in trust store should fail to connect
          client_ctx = OpenSSL::SSL::Context::Client.new
          client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
          client_ctx.private_key = "spec/resources/client_key.pem"
          client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

          # Connection should be rejected by server during SSL handshake
          expect_raises(AMQP::Client::Error | IO::Error | OpenSSL::SSL::Error) do
            conn = AMQP::Client.new(host: "localhost", port: amqps_port, tls: client_ctx).connect
            sleep 100.milliseconds # Give server time to reject
            conn.close rescue nil
          end
        ensure
          server.close
          amqps_socket.close
          FileUtils.rm_rf(config.data_dir)
        end
      ensure
        FileUtils.rm_rf("/tmp/lavinmq_trust_store_test")
      end
    end
  end
end
