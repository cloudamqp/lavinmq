require "spec"
require "../../src/stdlib/openssl_ktls"
require "../../src/stdlib/openssl_ktls_timeout"

# A peer that completes the handshake, sends one line, then goes silent, so the
# server side can hit its read_timeout on an otherwise healthy connection.
private def silent_peer_handshake(server_ctx : OpenSSL::SSL::Context::Server, &)
  tcp_server = TCPServer.new("localhost", 0)
  port = tcp_server.local_address.port
  stop = Channel(Nil).new
  done = Channel(Nil).new

  spawn do
    client_ctx = OpenSSL::SSL::Context::Client.new
    client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    tcp = TCPSocket.new("localhost", port)
    ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
    ssl.sync = true
    ssl.puts "hello"
    stop.receive
    ssl.close
    tcp.close
    done.send nil
  end

  tcp_client = tcp_server.accept
  ssl_client = OpenSSL::SSL::Socket::Server.new(tcp_client, server_ctx)
  ssl_client.gets.should eq "hello"

  yield ssl_client
ensure
  stop.try &.send nil
  ssl_client.try &.close
  tcp_client.try &.close
  tcp_server.try &.close
  done.try &.receive
end

private def server_context(ktls : Bool)
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  {% if OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
    ctx.add_options(OpenSSL::SSL::Options::ENABLE_KTLS) if ktls
  {% end %}
  ctx
end

describe OpenSSL::SSL::Socket do
  describe "read timeouts" do
    it "raises IO::TimeoutError when the peer goes silent" do
      silent_peer_handshake(server_context(ktls: false)) do |ssl|
        ssl.read_timeout = 100.milliseconds
        expect_raises(IO::TimeoutError) { ssl.read(Bytes.new(16)) }
      end
    end

    # Regression: with kTLS receive active the read goes through
    # OpenSSL::BIO -> OpenSSL::KTLS.read_record -> EventLoop#recvmsg, which
    # returns a timeout errno instead of raising. Unpatched, the timeout
    # surfaced as an OpenSSL::SSL::Error or an EOF, so callers relying on
    # IO::TimeoutError (AMQP heartbeats) never saw the timeout.
    {% if flag?(:linux) && OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
      it "raises IO::TimeoutError when the peer goes silent and kTLS receive is active" do
        pending!("kernel tls module not loaded (run: modprobe tls)") unless File.exists?("/sys/module/tls")

        silent_peer_handshake(server_context(ktls: true)) do |ssl|
          pending!("kTLS receive not engaged (status: #{ssl.ktls_status})") unless ssl.ktls_recv?

          ssl.read_timeout = 100.milliseconds
          expect_raises(IO::TimeoutError) { ssl.read(Bytes.new(16)) }
        end
      end
    {% end %}
  end
end
