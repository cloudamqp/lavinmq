require "./spec_helper"
require "http/client"

describe LavinMQ::Server do
  describe "mTLS (mutual TLS)" do
    it "should accept connections with valid client certificates" do
      with_amqp_server(tls: true, verify_peer: true, require_peer_cert: true) do |s|
        # Create client context with valid client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should successfully connect with valid client certificate
        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        conn.should_not be_nil
        channel = conn.channel
        channel.should_not be_nil
        conn.close
      end
    end

    it "should reject connections without client certificates when required" do
      with_amqp_server(tls: true, verify_peer: true, require_peer_cert: true) do |s|
        # Create client context WITHOUT client certificate
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        # Should fail to connect without client certificate
        expect_raises(OpenSSL::SSL::Error | AMQP::Client::Error) do
          AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect
        end
      end
    end

    it "should accept connections without client certificates when not required" do
      with_amqp_server(tls: true, verify_peer: true, require_peer_cert: false) do |s|
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
      with_amqp_server(tls: true, verify_peer: true, require_peer_cert: true) do |s|
        client_ctx = OpenSSL::SSL::Context::Client.new
        client_ctx.certificate_chain = "spec/resources/client_certificate.pem"
        client_ctx.private_key = "spec/resources/client_key.pem"
        client_ctx.ca_certificates = "spec/resources/ca_certificate.pem"

        conn = AMQP::Client.new(host: "localhost", port: amqp_port(s), tls: client_ctx).connect

        # Give server time to process connection
        sleep 100.milliseconds

        # Check that connection info was populated with certificate details
        connections = s.vhosts["/"].connections.to_a
        connections.size.should eq 1
        conn_info = connections.first.connection_info

        conn_info.ssl?.should be_true
        conn_info.ssl_verify?.should be_true
        conn_info.ssl_cn.should_not be_nil
        conn_info.ssl_sig_alg.should_not be_nil

        conn.close
      end
    end

    it "should work with MQTT over mTLS" do
      with_mqtts_server(verify_peer: true, require_peer_cert: true) do |s, port|
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
end
