require "socket"
require "openssl"
require "./proxy_protocol"
require "./config"
require "./ring_buffer_io"

module LavinMQ
  # TLS termination proxy that offloads TLS handshakes to dedicated execution context
  # Forwards decrypted traffic to internal TCP listener with PROXY protocol v2
  class TLSProxy
    Log = LavinMQ::Log.for "tls_proxy"

    @closed = false
    @server : TCPServer?
    @work_queue : Channel(TCPSocket)
    @connections : Channel(IO)
    @execution_context : Fiber::ExecutionContext::Parallel

    def initialize(
      @tls_context : OpenSSL::SSL::Context::Server,
      @max_concurrent_handshakes : Int32,
    )
      @work_queue = Channel(TCPSocket).new(@max_concurrent_handshakes * 2)
      @connections = Channel(IO).new(@max_concurrent_handshakes * 4)

      # Create parallel execution context with worker pool
      # Manages thread pool internally for better resource scheduling
      @execution_context = Fiber::ExecutionContext::Parallel.new("TLS workers", @max_concurrent_handshakes)
      @max_concurrent_handshakes.times do |i|
        @execution_context.spawn(name: "TLS worker #{i}") do
          worker_loop
        end
      end
    end

    def connections : Channel(IO)
      @connections
    end

    def listen(bind : String, port : Int32)
      server = TCPServer.new(bind, port)
      @server = server
      Log.info { "TLS proxy listening on #{bind}:#{port}" }
      Log.info { "TLS worker pool: #{@max_concurrent_handshakes} threads (parallel execution context)" }

      # Accept connections and push to work queue for worker pool
      loop do
        client = server.accept? || break
        if @closed
          client.close
          next
        end

        # Push to work queue (blocks if queue full, providing backpressure)
        @work_queue.send(client)
      end
    rescue ex : IO::Error
      Log.info { "TLS proxy stopped: #{ex.message}" }
    ensure
      @work_queue.close
    end

    def close
      @closed = true
      @server.try &.close
      @work_queue.close
    end

    # Worker loop running in parallel execution context
    # Pulls client sockets from work queue and handles TLS handshakes
    private def worker_loop
      loop do
        client = @work_queue.receive? || break
        handle_tls_client(client)
      rescue ex
        Log.error(exception: ex) { "TLS worker error" }
      end
    end

    private def handle_tls_client(client : TCPSocket)
      remote_addr = client.remote_address
      local_addr = client.local_address

      set_socket_options(client)

      # Perform TLS handshake (blocks worker thread, not main pool)
      ssl_client = OpenSSL::SSL::Socket::Server.new(client, @tls_context, sync_close: true)
      ssl_client.sync = false
      ssl_client.read_buffering = true
      tls_version = ssl_client.tls_version
      cipher = ssl_client.cipher

      server_io, proxy_io = LavinMQ.ring_buffer_io_pair(8192) # 8KB per direction, zero-syscall

      # Send PROXY protocol v2 header with TLS metadata
      proxy_header = ProxyProtocol::V2.new(remote_addr, local_addr)
      proxy_header.ssl = true
      proxy_header.ssl_version = tls_version
      proxy_header.ssl_cipher = cipher
      proxy_io.write_bytes proxy_header, IO::ByteFormat::NetworkEndian

      spawn(name: "TLS→Internal pipe #{remote_addr}") do
        copy_loop(proxy_io, ssl_client, "TLS→pipe #{remote_addr}")
      end
      spawn(name: "Internal pipe→TLS #{remote_addr}") do
        copy_loop(ssl_client, proxy_io, "pipe→TLS #{remote_addr}")
      end

      @connections.send server_io
    rescue ex : OpenSSL::SSL::Error
      Log.warn { "TLS handshake failed for #{remote_addr}: #{ex.message}" }
      ssl_client.try &.close rescue nil
      client.close rescue nil
    rescue ex : IO::Error
      Log.warn { "Connection error for #{remote_addr}: #{ex.message}" }
      ssl_client.try &.close rescue nil
      client.close rescue nil
    rescue ex
      Log.error(exception: ex) { "Unexpected error handling TLS client #{remote_addr}" }
      ssl_client.try &.close rescue nil
      client.close rescue nil
    end

    private def copy_loop(src : IO, dst : IO, fiber_name : String)
      buffer = Bytes.new(16384)
      index = 0
      loop do
        bytes_read = src.read(buffer)
        break if bytes_read == 0
        dst.write(buffer[0, bytes_read])
        dst.flush
        index += 1
      end
    rescue ex : IO::Error
      # Expected when connection closes
    ensure
      # Close destination to signal EOF to peer
      dst.close rescue nil
    end

    # TODO: Can we use the one from server.cr?
    private def set_socket_options(socket : TCPSocket)
      socket.sync = false
      socket.read_buffering = true
      socket.tcp_nodelay = true
      return if socket.remote_address.loopback?

      if keepalive = Config.instance.tcp_keepalive
        socket.keepalive = true
        socket.tcp_keepalive_idle = keepalive[0]
        socket.tcp_keepalive_interval = keepalive[1]
        socket.tcp_keepalive_count = keepalive[2]
      end
    end
  end
end
