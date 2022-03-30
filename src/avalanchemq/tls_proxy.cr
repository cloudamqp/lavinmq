require "socket"
require "openssl"
require "option_parser"
require "./config"
require "./version"
require "./proxy_protocol"

module AvalancheMQ
  class TLSProxy
    def initialize(@config : Config)
    end

    def run
      context = create_context
      s = TCPServer.new(@config.amqp_bind, @config.amqps_port, reuse_port: true)
      puts "Listening on #{s.local_address} (TLS)"
      while client = s.accept?
        remote_addr = client.remote_address
        spawn handle_connection(client, context, remote_addr)
      end
    rescue ex : IO::Error | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.inspect_with_backtrace}"
    end

    def handle_connection(client, context, remote_addr)
      set_socket_options(client)
      ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
      puts "Connected #{ssl_client.tls_version} #{ssl_client.cipher}"
      conn_info = ConnectionInfo.new(remote_addr, client.local_address)
      conn_info.ssl = true
      conn_info.ssl_version = ssl_client.tls_version
      conn_info.ssl_cipher = ssl_client.cipher
      proxy(ssl_client, conn_info)
    rescue ex
      puts "Error accepting TLS connection from #{remote_addr}: #{ex.inspect}"
      begin
        client.close
      rescue ex2
        puts "Error closing socket: #{ex2.inspect}"
      end
    end

    def proxy(client, conn_info)
      server = UNIXSocket.new(@config.unix_path)
      disable_buffering(client)
      disable_buffering(server)
      ProxyProtocol::V1.encode(conn_info, server) if @config.unix_proxy_protocol == 1
      spawn socket_copy_loop(client, server)
      spawn socket_copy_loop(server, client)
    end

    def socket_copy_loop(src, dst)
      buffer = uninitialized UInt8[8192]
      while (len = src.read(buffer.to_slice).to_i32) > 0
        dst.write buffer.to_slice[0, len]
      end
    rescue ex : IO::Error
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
      socket.tcp_nodelay = true if @config.tcp_nodelay
      @config.tcp_recv_buffer_size.try { |v| socket.recv_buffer_size = v }
      @config.tcp_send_buffer_size.try { |v| socket.send_buffer_size = v }
    end

    private def create_context
      context = OpenSSL::SSL::Context::Server.new
      # disable client initiated renegotiation
      context.add_options(OpenSSL::SSL::Options.new(0x40000000))
      context.certificate_chain = @config.tls_cert_path
      context.private_key = @config.tls_key_path
      context
    end
  end
end
