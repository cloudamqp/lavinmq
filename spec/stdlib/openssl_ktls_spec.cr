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
    ssl.puts "pong" if ssl.gets == "ping"
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

{% if OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
  describe OpenSSL::SSL::Context do
    it "doesn't enable kTLS by default" do
      ktls = OpenSSL::SSL::Options::ENABLE_KTLS
      OpenSSL::SSL::Context::Server.new.options.includes?(ktls).should be_false
      OpenSSL::SSL::Context::Client.new.options.includes?(ktls).should be_false
    end
  end
{% end %}

describe OpenSSL::SSL::Socket do
  describe "kTLS status" do
    it "reports no kTLS when ENABLE_KTLS option is not set" do
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"

      ktls_handshake(ctx) do |ssl|
        ssl.ktls_send?.should be_false
        ssl.ktls_recv?.should be_false
        ssl.ktls_status.should eq "off"
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
          ssl.ktls_status.should_not eq "off"
        end
      end
    {% end %}
  end

  describe "kTLS read timeout" do
    {% if flag?(:linux) && OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
      it "raises IO::TimeoutError and keeps the connection usable" do
        pending!("kernel tls module not loaded (run: modprobe tls)") unless File.exists?("/sys/module/tls")

        ctx = OpenSSL::SSL::Context::Server.new
        ctx.certificate_chain = "spec/resources/server_certificate.pem"
        ctx.private_key = "spec/resources/server_key.pem"
        ctx.add_options(OpenSSL::SSL::Options::ENABLE_KTLS)

        ktls_handshake(ctx) do |ssl|
          pending!("kTLS receive offload not active") unless ssl.ktls_recv?
          ssl.read_timeout = 50.milliseconds
          expect_raises(IO::TimeoutError) { ssl.gets }
          ssl.puts "ping"
          ssl.flush
          ssl.gets.should eq "pong"
        end
      end
    {% end %}
  end
end
