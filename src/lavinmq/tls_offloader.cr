require "openssl"
require "socket"
require "./proxy_protocol"
require "./connection_info"

module LavinMQ
  # TLS Offloader handles TLS termination in a separate execution context.
  # It accepts TLS connections, performs the SSL handshake, then forwards
  # decrypted traffic to the main server via IO::Stapled pipes with
  # PROXY protocol V2 headers to preserve client connection info and TLS metadata.
  class TLSOffloader
    Log = LavinMQ::Log.for "tls_offloader"

    # Represents a TLS connection that has been offloaded
    # The IO is ready to be used by the server (PROXY V2 header already sent)
    record OffloadedConnection, io : IO, protocol : Symbol

    def initialize(@tls_context : OpenSSL::SSL::Context::Server)
      @closed = false
      # Channel to send offloaded connections to the main execution context
      @connections = Channel(OffloadedConnection).new(64)
    end

    # Channel for receiving offloaded connections in the main execution context
    def connections : Channel(OffloadedConnection)
      @connections
    end

    def close
      @closed = true
      @connections.close
    end

    def closed?
      @closed
    end

    # Listen for TLS connections and forward decrypted traffic.
    # This method should be called from within the TLS execution context.
    # Offloaded connections are sent through the `connections` channel.
    def listen(server : TCPServer, protocol : Symbol)
      Log.info { "TLS offloader listening on #{server.local_address} for #{protocol}" }
      loop do
        break if @closed
        client = server.accept? || break
        spawn handle_connection(client, protocol), name: "TLS offload #{client.remote_address}"
      end
    rescue ex : IO::Error
      Log.error(exception: ex) { "Error in TLS offloader listener" } unless @closed
    end

    private def handle_connection(client : TCPSocket, protocol : Symbol)
      remote_addr = client.remote_address
      local_addr = client.local_address

      # Configure socket options before TLS handshake
      set_socket_options(client)

      # Perform TLS handshake
      ssl_client = OpenSSL::SSL::Socket::Server.new(client, @tls_context, sync_close: true)
      Log.debug { "#{remote_addr} TLS handshake complete: #{ssl_client.tls_version} #{ssl_client.cipher}" }

      # Create bidirectional in-process pipe pair
      # server_io is passed to the main context (the "plain text" side the server reads/writes)
      # offloader_io is the side we copy encrypted traffic to/from
      server_io, offloader_io = IO::Stapled.pipe

      # Send PROXY V2 header with TLS metadata through the pipe
      # The server will parse this to get client IP and TLS info
      ProxyProtocol::V2.new(remote_addr, local_addr,
        ssl_version: ssl_client.tls_version,
        ssl_cipher: ssl_client.cipher
      ).to_io(server_io)

      # Set up bidirectional copy between TLS socket and stapled pipe
      # These fibers run in the TLS execution context and stay alive
      # as long as the connection is active
      spawn(name: "TLS copy client->server") do
        copy_loop(ssl_client, offloader_io)
      end

      spawn(name: "TLS copy server->client") do
        copy_loop(offloader_io, ssl_client)
      end

      # Send the server-side IO to the main execution context via channel
      @connections.send(OffloadedConnection.new(server_io, protocol))
    rescue ex : OpenSSL::SSL::Error
      Log.debug { "TLS handshake failed from #{remote_addr}: #{ex.message}" }
      client.close rescue nil
    rescue ex
      Log.warn(exception: ex) { "Error handling TLS connection from #{remote_addr}" }
      client.close rescue nil
    end

    private def copy_loop(src : IO, dst : IO)
      buffer = Bytes.new(16384)
      loop do
        bytes_read = src.read(buffer)
        break if bytes_read == 0
        dst.write(buffer[0, bytes_read])
        dst.flush
      end
    rescue IO::Error
      # Connection closed, expected
    ensure
      # Close src first (may be TLS socket needing proper termination),
      # then dst (the pipe)
      src.close rescue nil
      dst.close rescue nil
    end

    private def set_socket_options(socket : TCPSocket)
      # Don't set keepalive for loopback (like the main server does)
      unless socket.remote_address.loopback?
        socket.keepalive = true
        socket.tcp_keepalive_idle = 60
        socket.tcp_keepalive_interval = 10
        socket.tcp_keepalive_count = 3
      end
      socket.tcp_nodelay = true
    end
  end
end
