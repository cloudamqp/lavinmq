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

# Completes the handshake, then the peer never reads again so the server's
# writes fill the socket buffers and block. The buffers are shrunk so that
# happens after a few KiB.
private def ktls_stalled_peer_handshake(server_ctx : OpenSSL::SSL::Context::Server, &)
  tcp_server = TCPServer.new("127.0.0.1", 0)
  port = tcp_server.local_address.port
  stop = Channel(Nil).new
  done = Channel(Nil).new

  spawn do
    client_ctx = OpenSSL::SSL::Context::Client.new
    client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    tcp = TCPSocket.new(Socket::Family::INET)
    tcp.recv_buffer_size = 4096 # before connect, so the advertised window stays small
    tcp.connect("127.0.0.1", port)
    ssl = OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
    stop.receive
    tcp.close
    done.send nil
  end

  tcp_client = tcp_server.accept
  tcp_client.send_buffer_size = 4096
  ssl_client = OpenSSL::SSL::Socket::Server.new(tcp_client, server_ctx)

  yield ssl_client, tcp_client
ensure
  if done
    stop.try &.send nil
    done.receive
  end
  # The peer is gone and the send buffer may be full, so shutdown can fail
  begin
    ssl_client.try &.close
  rescue IO::Error
  end
  tcp_client.try &.close
  tcp_server.try &.close
end

# Completes the handshake, then the peer resets the connection
private def ktls_reset_peer_handshake(server_ctx : OpenSSL::SSL::Context::Server, &)
  tcp_server = TCPServer.new("127.0.0.1", 0)
  port = tcp_server.local_address.port
  accepted = Channel(Nil).new
  reset = Channel(Nil).new

  spawn do
    client_ctx = OpenSSL::SSL::Context::Client.new
    client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    tcp = TCPSocket.new("127.0.0.1", port)
    OpenSSL::SSL::Socket::Client.new(tcp, client_ctx)
    accepted.receive
    tcp.linger = 0
    tcp.close
    reset.send nil
  end

  tcp_client = tcp_server.accept
  ssl_client = OpenSSL::SSL::Socket::Server.new(tcp_client, server_ctx, sync_close: true)
  accepted.send nil
  reset.receive

  yield ssl_client
ensure
  ssl_client.try &.close
  tcp_server.try &.close
end

private def ktls_server_context
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  ctx.add_options(OpenSSL::SSL::Options::ENABLE_KTLS)
  ctx
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

        ktls_handshake(ktls_server_context) do |ssl|
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

        ktls_handshake(ktls_server_context) do |ssl|
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

  describe "kTLS peer reset" do
    {% if flag?(:linux) && OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
      it "raises IO::Error on read instead of returning EOF" do
        pending!("kernel tls module not loaded (run: modprobe tls)") unless File.exists?("/sys/module/tls")

        ktls_reset_peer_handshake(ktls_server_context) do |ssl|
          pending!("kTLS receive offload not active") unless ssl.ktls_recv?
          expect_raises(IO::Error) { ssl.gets }
        end
      end
    {% end %}
  end

  describe "write timeout" do
    # Baseline: plain TLS, no kTLS
    it "raises IO::TimeoutError when the peer stops reading" do
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"

      ktls_stalled_peer_handshake(ctx) do |ssl|
        ssl.write_timeout = 50.milliseconds

        chunk = Bytes.new(64 * 1024, 'x'.ord.to_u8)
        expect_raises(IO::TimeoutError) do
          # Bounded so a missing timeout fails instead of hanging
          256.times { ssl.write(chunk) }
          fail "write never blocked"
        end
      end
    end
  end

  describe "kTLS write timeout" do
    {% if flag?(:linux) && OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
      # Application data: OpenSSL hands the plaintext to Socket#write and the
      # kernel encrypts it in the send handler that TLS_TX installs.
      it "raises IO::TimeoutError when the peer stops reading" do
        pending!("kernel tls module not loaded (run: modprobe tls)") unless File.exists?("/sys/module/tls")

        ktls_stalled_peer_handshake(ktls_server_context) do |ssl|
          pending!("kTLS send offload not active") unless ssl.ktls_send?
          ssl.write_timeout = 50.milliseconds

          chunk = Bytes.new(64 * 1024, 'x'.ord.to_u8)
          expect_raises(IO::TimeoutError) do
            # Bounded so a missing timeout fails instead of hanging
            256.times { ssl.write(chunk) }
            fail "write never blocked"
          end
        end
      end

      # Control records (alerts, handshake messages) bypass Socket#write and go
      # through KTLS.send_ctrl_message, which returns an Errno to OpenSSL
      # instead of raising. close sends a close_notify alert that way, and
      # OpenSSL retried it forever when it timed out. close rescues IO::Error,
      # so a timeout lets it return instead.
      it "closes when sending close_notify times out" do
        pending!("kernel tls module not loaded (run: modprobe tls)") unless File.exists?("/sys/module/tls")

        ktls_stalled_peer_handshake(ktls_server_context) do |ssl, tcp|
          pending!("kTLS send offload not active") unless ssl.ktls_send?
          ssl.write_timeout = 50.milliseconds
          tcp.write_timeout = 50.milliseconds

          # Fill the socket from under OpenSSL so its state stays clean; the
          # kernel sends these bytes as application data records.
          chunk = Bytes.new(64 * 1024, 'x'.ord.to_u8)
          expect_raises(IO::TimeoutError) do
            256.times { tcp.write(chunk) }
            fail "write never blocked"
          end

          result = Channel(Exception?).new(1)
          spawn do
            ssl.close
            result.send nil
          rescue ex
            result.send ex
          end

          select
          when ex = result.receive
            # The peer never sends close_notify, so returning at all means
            # the write timed out: a completed write would wait to read it
            ex.should be_nil
          when timeout(5.seconds)
            fail "close never returned"
          end
        end
      end
    {% end %}
  end
end
