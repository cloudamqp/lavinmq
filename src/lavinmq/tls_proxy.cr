require "socket"
require "openssl"
require "./proxy_protocol"
require "./config"

module LavinMQ
  # TLS termination proxy that offloads TLS handshakes to dedicated execution context
  # Forwards decrypted traffic to internal TCP listener with PROXY protocol v2
  class TLSProxy
    Log = LavinMQ::Log.for "tls_proxy"

    @closed = false
    @server : TCPServer?
    @work_queue : Channel(TCPSocket)

    def initialize(
      @tls_context : OpenSSL::SSL::Context::Server,
      @internal_socket_path : String,
      @max_concurrent_handshakes : Int32
    )
      @work_queue = Channel(TCPSocket).new(@max_concurrent_handshakes * 2)

      # Create worker pool with isolated execution contexts
      # Each worker runs in its own OS thread, pulling work from the queue
      @max_concurrent_handshakes.times do |i|
        Fiber::ExecutionContext::Isolated.new("TLS worker #{i}") do
          worker_loop
        end
      end
    end

    def listen(bind : String, port : Int32)
      server = TCPServer.new(bind, port)
      @server = server
      Log.info { "TLS proxy listening on #{bind}:#{port}, forwarding to #{@internal_socket_path}" }
      Log.info { "TLS worker pool: #{@max_concurrent_handshakes} isolated threads" }

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

    # Worker loop running in isolated execution context
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

      # Perform TLS handshake (blocks isolated thread, not main pool)
      ssl_client = OpenSSL::SSL::Socket::Server.new(client, @tls_context, sync_close: true)
      ssl_client.sync = false
      ssl_client.read_buffering = true
      tls_version = ssl_client.tls_version
      cipher = ssl_client.cipher

      Log.debug { "#{remote_addr} connected with #{tls_version} #{cipher}" }

      # Connect to internal unix socket listener
      internal = UNIXSocket.new(@internal_socket_path)
      set_buffer_size(internal)

      # Send PROXY protocol v2 header with TLS metadata
      proxy_header = ProxyProtocol::V2.new(remote_addr, local_addr)
      proxy_header.ssl = true
      proxy_header.ssl_version = tls_version
      proxy_header.ssl_cipher = cipher
      internal.write_bytes proxy_header, IO::ByteFormat::NetworkEndian

      # Handshake complete - spawn bidirectional copy in main pool and return worker to queue
      # This allows the worker thread to immediately process the next TLS handshake
      spawn(name: "TLS proxy #{remote_addr}") do
        forward_bidirectional(ssl_client, internal, remote_addr)
      end
    rescue ex : OpenSSL::SSL::Error
      Log.debug { "TLS handshake failed for #{remote_addr}: #{ex.message}" }
      ssl_client.try &.close rescue nil
      client.close rescue nil
    rescue ex : IO::Error
      Log.debug { "Connection error for #{remote_addr}: #{ex.message}" }
      internal.try &.close rescue nil
      ssl_client.try &.close rescue nil
      client.close rescue nil
    end

    # Bidirectional forwarding runs in main fiber pool (not isolated thread)
    # This allows worker threads to return immediately after TLS handshake
    private def forward_bidirectional(ssl_client : OpenSSL::SSL::Socket::Server, internal : UNIXSocket, remote_addr : Socket::Address)
      buffer = Bytes.new(16384)

      spawn(name: "TLS→Plain #{remote_addr}") do
        loop do
          bytes_read = ssl_client.read(buffer)
          break if bytes_read == 0
          internal.write(buffer[0, bytes_read])
          internal.flush
        end
      rescue IO::Error
        # Expected when connection closes
      end

      loop do
        bytes_read = internal.read(buffer)
        break if bytes_read == 0
        ssl_client.write(buffer[0, bytes_read])
        ssl_client.flush
      end

      Log.debug { "#{remote_addr} disconnected" }
    rescue ex : IO::Error
      Log.debug { "Connection error for #{remote_addr}: #{ex.message}" }
    ensure
      internal.close rescue nil
      ssl_client.close rescue nil
    end

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

    private def set_buffer_size(socket : UNIXSocket)
      socket.sync = false
      socket.read_buffering = true
      socket.buffer_size = 131_072
    end
  end
end
