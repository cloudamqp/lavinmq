require "socket"
require "openssl"
require "./avalanchemq/config"
require "./avalanchemq/proxy_protocol"

class AvalancheMQ::TLSProxy
  def start
    ctx = create_context
    listen_tls("::", 5671, ctx)
  end

  def create_context
    context = OpenSSL::SSL::Context::Server.new
    context.add_options(OpenSSL::SSL::Options.new(0x40000000)) # disable client initiated renegotiation
    context
  end

  def listen_tls(bind, port, context)
    s = TCPServer.new(bind, port, reuse_port: true)
    puts "Listening on #{s.local_address} (TLS)"
    while client = s.accept?
      remote_addr = client.remote_address
      begin
        set_socket_options(client)
        ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
        puts "Connected #{ssl_client.tls_version} #{ssl_client.cipher}"
        spawn handle_connection(ssl_client), name: "Server#handle_connection(tls)"
      rescue ex
        puts "Error accepting TLS connection from #{remote_addr}: #{ex.inspect}"
        begin
          client.close
        rescue ex2
          puts "Error closing socket: #{ex2.inspect}"
        end
      end
    end
  rescue ex : IO::Error | OpenSSL::Error
    abort "Unrecoverable error in TLS listener: #{ex.inspect_with_backtrace}"
  end

  def handle_connection(client)
    server = TCPSocket.new("127.0.0.1", 5672)
    disable_buffering(client)
    disable_buffering(server)
    spawn socket_copy_loop(client, server)
    spawn socket_copy_loop(server, client)
  end

  def socket_copy_loop(src, dst)
    buffer = uninitialized UInt8[8192]
    while (len = src.read(buffer.to_slice).to_i32) > 0
      dst.write buffer.to_slice[0, len]
    end
  ensure
    src.close
    dst.close
  end

  private def disable_buffering(socket)
    socket.sync = true
    socket.read_buffering = false
  end

  private def set_socket_options(socket)
    unless socket.remote_address.loopback?
      if keepalive = Config.instance.tcp_keepalive
        socket.keepalive = true
        socket.tcp_keepalive_idle = keepalive[0]
        socket.tcp_keepalive_interval = keepalive[1]
        socket.tcp_keepalive_count = keepalive[2]
      end
    end
    socket.tcp_nodelay = true if Config.instance.tcp_nodelay
    Config.instance.tcp_recv_buffer_size.try { |v| socket.recv_buffer_size = v }
    Config.instance.tcp_send_buffer_size.try { |v| socket.send_buffer_size = v }
  end
end

AvalancheMQ::TLSProxy.new.start
