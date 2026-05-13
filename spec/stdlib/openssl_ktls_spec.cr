require "spec"
require "../../src/stdlib/openssl_ktls"

private def ktls_handshake(server_ctx : OpenSSL::SSL::Context::Server, &)
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
  ssl_client = OpenSSL::SSL::Socket::Server.new(tcp_client, server_ctx)
  ssl_client.gets.should eq "hello"
  ssl_client.puts "world"

  yield ssl_client
ensure
  ssl_client.try &.close
  tcp_client.try &.close
  tcp_server.try &.close
  done.try &.receive
end

describe OpenSSL::SSL::Socket do
  describe "kTLS status" do
    it "reports no kTLS when ENABLE_KTLS option is not set" do
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"

      ktls_handshake(ctx) do |ssl|
        ssl.ktls_send?.should be_false
        ssl.ktls_recv?.should be_false
        ssl.ktls_status.should be_nil
      end
    end

    {% if flag?(:linux) && OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
      it "engages kTLS on sockets when ENABLE_KTLS option is set" do
        pending!("kernel tls module not loaded (run: modprobe tls)") unless File.exists?("/sys/module/tls")

        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = "spec/resources/server_certificate.pem"
        ctx.private_key = "spec/resources/server_key.pem"
        ctx.add_options(OpenSSL::SSL::Options::ENABLE_KTLS)

        ktls_handshake(ctx) do |ssl|
          ssl.ktls_send?.should be_true
          ssl.ktls_status.should_not be_nil
        end
      end
    {% end %}
  end
end
