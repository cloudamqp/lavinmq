require "spec"
require "../../src/stdlib/openssl_keylog"
require "../../src/stdlib/openssl_sni"

describe OpenSSL::SSL::Context::Server do
  describe "#keylog_file=" do
    it "logs TLS keys during handshake" do
      keylog_path = File.tempname("keylog", ".txt")
      begin
        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = "spec/resources/server_certificate.pem"
        ctx.private_key = "spec/resources/server_key.pem"
        ctx.keylog_file = keylog_path

        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port

        done = Channel(Nil).new

        spawn do
          client_ctx = OpenSSL::SSL::Context::Client.new
          client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
          tcp = TCPSocket.new("localhost", port)
          ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
          ssl.sync = true
          ssl.puts "hello"
          ssl.gets
          ssl.close
          tcp.close
          done.send nil
        end

        tcp_client = tcp_server.accept
        ssl_client = OpenSSL::SSL::Socket::Server.new(tcp_client, ctx)
        ssl_client.gets.should eq "hello"
        ssl_client.puts "world"
        ssl_client.close
        tcp_client.close
        tcp_server.close

        done.receive

        content = File.read(keylog_path)
        content.should_not be_empty
        content.should contain("CLIENT_HANDSHAKE_TRAFFIC_SECRET")
      ensure
        ctx.try(&.keylog_file=(nil))
        File.delete?(keylog_path)
      end
    end

    it "logs TLS keys for multiple SNI hosts to same file without interleaving" do
      keylog_path = File.tempname("keylog_shared", ".txt")
      begin
        # Default context
        default_ctx = OpenSSL::SSL::Context::Server.new
        default_ctx.certificate_chain = "spec/resources/server_certificate.pem"
        default_ctx.private_key = "spec/resources/server_key.pem"
        default_ctx.keylog_file = keylog_path

        # SNI context for "localhost"
        sni_ctx = OpenSSL::SSL::Context::Server.new
        sni_ctx.certificate_chain = "spec/resources/server_certificate.pem"
        sni_ctx.private_key = "spec/resources/server_key.pem"
        sni_ctx.keylog_file = keylog_path

        default_ctx.set_sni_callback do |hostname|
          hostname == "localhost" ? sni_ctx : nil
        end

        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port

        done = Channel(Nil).new(2)

        # Spawn two concurrent clients
        2.times do
          spawn do
            client_ctx = OpenSSL::SSL::Context::Client.new
            client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
            tcp = TCPSocket.new("localhost", port)
            ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx, hostname: "localhost")
            ssl.sync = true
            ssl.puts "hello"
            ssl.gets
            ssl.close
            tcp.close
            done.send nil
          end
        end

        # Accept two connections
        2.times do
          tcp_client = tcp_server.accept
          spawn do
            ssl_client = OpenSSL::SSL::Socket::Server.new(tcp_client, default_ctx)
            ssl_client.gets
            ssl_client.puts "world"
            ssl_client.close
            tcp_client.close
          end
        end

        2.times { done.receive }
        tcp_server.close

        content = File.read(keylog_path)
        content.should_not be_empty
        content.should contain("CLIENT_HANDSHAKE_TRAFFIC_SECRET")

        # Verify no interleaved/empty lines - each line should match NSS keylog format
        lines = content.split("\n")
        lines.each do |line|
          next if line.empty? # last line after final newline
          line.should match(/^[A-Z_0-9]+ [0-9a-f]+ [0-9a-f]+$/)
        end
      ensure
        default_ctx.try(&.keylog_file=(nil))
        sni_ctx.try(&.keylog_file=(nil))
        File.delete?(keylog_path)
      end
    end
  end
end
