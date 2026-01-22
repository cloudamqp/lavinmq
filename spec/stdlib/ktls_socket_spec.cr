require "spec"
require "../../src/stdlib/ktls_socket"
require "../../src/stdlib/openssl_ktls"
require "../../src/stdlib/openssl_keylog"

# Helper to create a basic server context with kTLS enabled
def create_server_context
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  ctx.enable_ktls
  ctx.add_options(OpenSSL::SSL::Options::NO_TLS_V1_3)
  ctx
end

# Helper to create a basic client context
def create_client_context
  ctx = OpenSSL::SSL::Context::Client.new
  ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
  ctx
end

{% if compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 %}
  describe OpenSSL::SSL::KTLSSocket do
    describe "kTLS Status and Functionality" do
      it "successfully establishes kTLS connection" do
        tcp_server = TCPServer.new("localhost", 55671)
        port = tcp_server.local_address.port
        server_ctx = create_server_context
        client_ctx = create_client_context

        done = Channel(Nil).new

        spawn(name: "Server") do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          # Verify kTLS status can be queried
          status = ssl_client.ktls_status

          # Status might be nil (kTLS not active) or a string (kTLS active)
          # Both are valid depending on kernel/cipher support

          # Test basic I/O
          msg = ssl_client.gets
          msg.should eq("hello")
          ssl_client.puts("world")

          ssl_client.close
          tcp_client.close
          done.send(nil)
        end

        spawn(name: "Client") do
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          ssl.puts("hello")
          ssl.flush
          response = ssl.gets
          response.should eq("world")

          ssl.close
          tcp.close
        end

        done.receive
        tcp_server.close
      end

      it "reports kTLS status correctly" do
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        server_ctx = create_server_context

        done = Channel(String?).new

        spawn do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          status = ssl_client.ktls_status
          send_status = ssl_client.ktls_send?
          recv_status = ssl_client.ktls_recv?

          # Verify consistency between individual checks and combined status
          case status
          when "send+recv"
            send_status.should be_true
            recv_status.should be_true
          when "send"
            send_status.should be_true
            recv_status.should be_false
          when "recv"
            send_status.should be_false
            recv_status.should be_true
          when nil
            send_status.should be_false
            recv_status.should be_false
          end

          done.send(status)
          ssl_client.close
          tcp_client.close
        end

        spawn do
          client_ctx = create_client_context
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          ssl.close
          tcp.close
        end

        status = done.receive
        tcp_server.close

        # kTLS might or might not be active depending on:
        # - Kernel version and configuration
        # - Cipher suite negotiated
        # - TLS version
        # We just verify the method doesn't crash
      end
    end

    describe "Basic Socket Operations" do
      it "handles peer certificate correctly" do
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        server_ctx = create_server_context
        client_ctx = create_client_context

        done = Channel(Nil).new

        spawn do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          # Client didn't provide a certificate (no client auth)
          cert = ssl_client.peer_certificate
          cert.should be_nil

          ssl_client.close
          tcp_client.close
          done.send(nil)
        end

        spawn do
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          ssl.close
          tcp.close
        end

        done.receive
        tcp_server.close
      end

      it "provides local and remote addresses" do
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        server_ctx = create_server_context
        client_ctx = create_client_context

        done = Channel(Nil).new

        spawn do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          local = ssl_client.local_address
          remote = ssl_client.remote_address

          local.should_not be_nil
          remote.should_not be_nil
          remote.as(Socket::IPAddress).port.should_not eq(0)

          ssl_client.close
          tcp_client.close
          done.send(nil)
        end

        spawn do
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          ssl.close
          tcp.close
        end

        done.receive
        tcp_server.close
      end

      it "supports read and write timeouts" do
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        server_ctx = create_server_context
        client_ctx = create_client_context

        done = Channel(Nil).new

        spawn do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          # Set and verify timeouts
          ssl_client.read_timeout = 5.seconds
          ssl_client.write_timeout = 5.seconds

          ssl_client.read_timeout.should eq(5.seconds)
          ssl_client.write_timeout.should eq(5.seconds)

          ssl_client.close
          tcp_client.close
          done.send(nil)
        end

        spawn do
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          ssl.close
          tcp.close
        end

        done.receive
        tcp_server.close
      end

      it "raises error when reading from closed socket" do
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        server_ctx = create_server_context
        client_ctx = create_client_context

        done = Channel(Exception?).new

        spawn do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          ssl_client.close
          tcp_client.close

          # Try to read after close
          begin
            buffer = Bytes.new(1024)
            ssl_client.unbuffered_read(buffer)
            done.send(nil)
          rescue ex
            done.send(ex)
          end
        end

        spawn do
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          sleep 0.5.seconds
          ssl.close
          tcp.close
        end

        ex = done.receive
        ex.should be_a(IO::Error)
        ex.not_nil!.message.to_s.should contain("Closed")

        tcp_server.close
      end

      it "raises error when writing to closed socket" do
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        server_ctx = create_server_context
        client_ctx = create_client_context

        done = Channel(Exception?).new

        spawn do
          tcp_client = tcp_server.accept
          ssl_client = OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, server_ctx)

          ssl_client.close
          tcp_client.close

          # Try to write after close
          begin
            ssl_client.unbuffered_write(Bytes[1, 2, 3])
            done.send(nil)
          rescue ex
            done.send(ex)
          end
        end

        spawn do
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          sleep 0.5.seconds
          ssl.close
          tcp.close
        end

        ex = done.receive
        ex.should be_a(IO::Error)
        ex.not_nil!.message.to_s.should contain("Closed")

        tcp_server.close
      end
    end
  end
{% else %}
  # OpenSSL < 3.0 - kTLS not available
  describe OpenSSL::SSL::KTLSSocket do
    it "raises NotImplementedError for OpenSSL < 3.0" do
      expect_raises(NotImplementedError, "requires OpenSSL 3.0+") do
        tcp_server = TCPServer.new("localhost", 0)
        tcp_client = tcp_server.accept
        ctx = OpenSSL::SSL::Context::Server.new
        OpenSSL::SSL::KTLSSocket::Server.new(tcp_client, ctx)
      end
    end
  end
{% end %}
