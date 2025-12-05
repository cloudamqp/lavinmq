require "spec"
require "../../src/stdlib/openssl_keylog"

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
  end
end
