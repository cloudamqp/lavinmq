require "spec"
require "../../src/stdlib/openssl_socket_close_free"

private def tls_pair(sync_close = true, &)
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = "spec/resources/server_certificate.pem"
  ctx.private_key = "spec/resources/server_key.pem"
  tcp_server = TCPServer.new("localhost", 0)
  clients = Channel(OpenSSL::SSL::Socket::Client).new(1)
  spawn do
    client_ctx = OpenSSL::SSL::Context::Client.new
    client_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    tcp = TCPSocket.new("localhost", tcp_server.local_address.port)
    clients.send OpenSSL::SSL::Socket::Client.new(tcp, client_ctx, sync_close: true)
  end
  tcp = tcp_server.accept
  server = OpenSSL::SSL::Socket::Server.new(tcp, ctx, sync_close: sync_close)
  client = clients.receive
  yield server, client
ensure
  client.try &.close
  server.try &.close
  tcp.try &.close
  tcp_server.try &.close
end

describe OpenSSL::SSL::Socket do
  it "frees the SSL object when it closes, once" do
    tls_pair do |server, _client|
      server.@ssl_freed.get.should be_false
      server.close
      server.@ssl_freed.get.should be_true
      server.finalize # what the GC would call: must not free it again
    end
  end

  it "keeps the SSL object while another call is still in OpenSSL" do
    tls_pair do |server, _client|
      server.@ssl_calls.add(1) # as if another fiber were suspended in SSL_read
      server.close
      server.@ssl_freed.get.should be_false
      server.@ssl_calls.sub(1)
      server.finalize
    end
  end

  it "frees it once a suspended read and the close have both left OpenSSL" do
    # Without sync_close the close waits for the peer's close_notify, and both the read and
    # the close stay in OpenSSL until the client closes
    tls_pair(sync_close: false) do |server, client|
      done = Channel(Nil).new
      spawn do
        server.gets
      rescue IO::Error
      ensure
        done.send nil
      end
      spawn do
        server.close
      ensure
        done.send nil
      end
      Fiber.yield
      server.closed?.should be_true
      server.@ssl_calls.get.should eq 2
      server.@ssl_freed.get.should be_false
      client.close
      2.times { done.receive }
      server.@ssl_calls.get.should eq 0
      server.@ssl_freed.get.should be_true
    end
  end
end
