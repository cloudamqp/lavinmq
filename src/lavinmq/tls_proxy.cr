require "socket"
require "openssl"
require "./proxy_protocol"
require "./config"
require "./ring_buffer_io"
require "./mutex_buffered_io"
require "./fiber_ring_buffer_io"
require "./channel_ring_buffer_io"
require "./tls_socket_wrapper"

module LavinMQ
  # TLS termination proxy that offloads TLS handshakes to dedicated execution context
  # Forwards decrypted traffic to internal TCP listener with PROXY protocol v2
  class TLSProxy
    Log = LavinMQ::Log.for "tls_proxy"

    @closed = false
    @server : TCPServer?
    @work_queue : Channel(TCPSocket)
    @connections : Channel(TLSSocketWrapper)
    @execution_context : Fiber::ExecutionContext::Parallel
    @wg : WaitGroup = WaitGroup.new(0)

    def initialize(
      @tls_context : OpenSSL::SSL::Context::Server,
      @max_concurrent_handshakes : Int32,
    )
      @work_queue = Channel(TCPSocket).new(@max_concurrent_handshakes * 2)
      @connections = Channel(TLSSocketWrapper).new(@max_concurrent_handshakes * 4)

      # Create parallel execution context with worker pool
      # Manages thread pool internally for better resource scheduling
      @execution_context = Fiber::ExecutionContext::Parallel.new("TLS workers", @max_concurrent_handshakes)
      @max_concurrent_handshakes.times do |i|
        @execution_context.spawn(name: "TLS worker #{i}") do
          @wg.add(1)
          worker_loop
        ensure
          @wg.done
        end
      end
    end

    def connections : Channel(TLSSocketWrapper)
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
      Log.info { "TLS proxy shutting down" }
      @work_queue.close
    end

    def close
      @closed = true
      @server.try &.close
      @work_queue.close
      @connections.close
      @wg.wait
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
      ssl_client.read_buffering = false
      tls_version = ssl_client.tls_version
      cipher = ssl_client.cipher

      conn_info = ConnectionInfo.new(remote_addr, local_addr)
      conn_info.ssl = true
      conn_info.ssl_version = ssl_client.tls_version
      conn_info.ssl_cipher = ssl_client.cipher

      wrapped_client = TLSSocketWrapper.new(ssl_client, conn_info)
      wrapped_client.sync = false
      wrapped_client.read_buffering = true

      # server_io, proxy_io = LavinMQ.fiber_ring_buffer_io_pair(8192) # 8KB per direction, zero-syscall

      # Send PROXY protocol v2 header with TLS metadata
      # proxy_header = ProxyProtocol::V2.new(remote_addr, local_addr)
      # proxy_header.ssl = true
      # proxy_header.ssl_version = tls_version
      # proxy_header.ssl_cipher = cipher
      # wrapped_client.write_bytes proxy_header, IO::ByteFormat::NetworkEndian

      # @wg.spawn(name: "Internal pipe→TLS #{remote_addr}") do
      #   # Drain ring buffer directly to TLS socket (server responses → TLS client)
      #   drain_loop(proxy_io, ssl_client, "pipe→TLS #{remote_addr}")
      # end

      # @wg.spawn(name: "TLS→Internal pipe #{remote_addr}") do
      #   # Fill ring buffer directly from TLS socket (TLS client → server)
      #   fill_loop(proxy_io, ssl_client, "TLS→pipe #{remote_addr}")
      # end

      @connections.send wrapped_client
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

    # Drain ring buffer directly to destination IO (zero-copy)
    private def drain_loop(ring_buffer : FiberRingBufferIO, dst : IO, fiber_name : String)
      loop do
        bytes_written = ring_buffer.drain(dst)
        break if bytes_written == 0
        dst.flush
      end
    rescue ex : IO::Error
      # Expected when connection closes
    ensure
      dst.close rescue nil
    end

    # Fill ring buffer directly from source IO (zero-copy)
    private def fill_loop(ring_buffer : FiberRingBufferIO, src : IO, fiber_name : String)
      loop do
        bytes_read = ring_buffer.fill(src)
        break if bytes_read == 0
      end
    rescue ex : IO::Error
      # Expected when connection closes
    ensure
      ring_buffer.close rescue nil
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
