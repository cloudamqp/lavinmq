require "socket"
require "openssl"
require "wait_group"
require "./server"

module LavinMQ
  enum Protocol
    AMQP
    MQTT
  end

  abstract class ProtocolServer
    @listeners = Array(Socket::Server).new
    @tls_contexts = Hash(TCPServer, OpenSSL::SSL::Context::Server).new
    @closed = BoolChannel.new(false)
    @config : Config
    @listening = false
    Log = LavinMQ::Log.for "server"

    def initialize(@server : LavinMQ::Server, @config : Config, @protocol : Protocol)
    end

    def closed? : Bool
      @closed.value
    end

    def listening? : Bool
      @listening
    end

    def bind_tcp(s : TCPServer)
      bind(s)
      addr = s.local_address
      Log.info { "Bound #{@protocol} to #{addr}" }
      addr
    end

    def bind_tcp(bind : String, port : Int)
      bind_tcp(TCPServer.new(bind, port))
    rescue ex : Socket::BindError
      abort "Error: #{ex.message}"
    end

    def bind_tls(s : TCPServer, context : OpenSSL::SSL::Context::Server)
      bind(s)
      @tls_contexts[s] = context
      addr = s.local_address
      Log.info { "Bound #{@protocol} to #{addr} (TLS)" }
      addr
    end

    def bind_tls(bind : String, port : Int, context : OpenSSL::SSL::Context::Server)
      bind_tls(TCPServer.new(bind, port), context)
    rescue ex : Socket::BindError
      abort "Error: #{ex.message}"
    end

    def bind_unix(path : String)
      File.delete?(path)
      s = UNIXServer.new(path)
      File.chmod(path, 0o666)
      bind(s)
      addr = s.local_address
      Log.info { "Bound #{@protocol} to #{addr}" }
      addr
    rescue ex : Socket::BindError
      abort "Error: #{ex.message}"
    end

    def listen : Nil
      listening = false
      raise "Can't re-start closed #{@protocol} server" if closed?
      raise "Can't start #{@protocol} server with no sockets to listen to, use bind first" if @listeners.empty?
      raise "Can't start running #{@protocol} server" if listening?

      @listening = true
      listening = true
      sockets = @listeners.dup

      WaitGroup.wait do |wg|
        sockets.each do |socket|
          wg.spawn(name: "#{@protocol} listener") do
            case socket
            when TCPServer
              if context = @tls_contexts[socket]?
                listen_tls(socket, context)
              else
                listen_tcp(socket)
              end
            when UNIXServer
              listen_unix(socket)
            else
              raise "Unexpected listener '#{socket.class}'"
            end
          end
        end
      end
    ensure
      @listening = false if listening
    end

    private def bind(s : Socket::Server)
      raise "Can't add socket to running #{@protocol} server" if listening?
      raise "Can't add socket to closed #{@protocol} server" if closed?

      @listeners << s unless @listeners.includes?(s)
    end

    private def listen_tcp(s : TCPServer)
      loop do
        client = s.accept? || break
        next client.close if closed? || @server.closed?
        accept_tcp(client)
      end
    rescue ex : IO::Error
      return if closed?
      abort "Unrecoverable error in listener: #{ex.inspect_with_backtrace}"
    end

    private def listen_unix(s : UNIXServer)
      loop do # do not try to use while
        client = s.accept? || break
        next client.close if closed? || @server.closed?
        accept_unix(client)
      end
    rescue ex : IO::Error
      return if closed?
      abort "Unrecoverable error in unix listener: #{ex.inspect_with_backtrace}"
    end

    private def listen_tls(s : TCPServer, context : OpenSSL::SSL::Context::Server)
      loop do # do not try to use while
        client = s.accept? || break
        next client.close if closed? || @server.closed?
        accept_tls(client, context)
      end
    rescue ex : IO::Error | OpenSSL::Error
      return if closed?
      abort "Unrecoverable error in TLS listener: #{ex.inspect_with_backtrace}"
    end

    def listeners
      @listeners.map do |l|
        case l
        when UNIXServer
          addr = l.local_address
          {
            "path":     addr.path,
            "protocol": @protocol,
          }
        when TCPServer
          addr = l.local_address
          {
            "ip_address": addr.address,
            "protocol":   @protocol,
            "port":       addr.port,
          }
        else raise "Unexpected listener '#{l.class}'"
        end
      end
    end

    def close
      return if closed?
      @closed.set(true)
      Log.debug { "Closing #{@protocol} listeners" }
      @listeners.each do |listener|
        listener.close rescue nil
      end
      @listeners.clear
      @tls_contexts.clear
      @listening = false
    end

    abstract def handle_connection(socket, connection_info)

    private def accept_tcp(client)
      spawn(name: "Accept TCP socket") do
        remote_address = client.remote_address
        set_socket_options(client)
        set_buffer_size(client)
        conn_info = extract_conn_info(client)
        handle_connection(client, conn_info)
      rescue ex
        Log.warn { "Error accepting connection from #{remote_address}: #{ex.message}" }
        client.close rescue nil
      end
    end

    private def accept_unix(client)
      spawn(name: "Accept UNIX socket") do
        remote_address = client.remote_address
        set_buffer_size(client)
        if conn_info = ProxyProtocol.parse(client)
          # PROXY protocol over unix socket
        else
          conn_info = ConnectionInfo.local # TODO: use unix socket address, don't fake local
        end
        handle_connection(client, conn_info)
      rescue ex
        Log.warn(exception: ex) { "Error accepting connection from #{remote_address}" }
        client.close rescue nil
      end
    end

    private def accept_tls(client, context)
      spawn(name: "Accept TLS socket") do
        remote_addr = client.remote_address
        set_socket_options(client)
        ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
        Log.info { "#{remote_addr} connected with #{ssl_client.tls_version} #{ssl_client.cipher} kTLS=#{ssl_client.ktls_status}" }
        handle_tls_connection(ssl_client, client.local_address, remote_addr)
      rescue ex
        Log.warn(exception: ex) { "Error accepting TLS connection from #{remote_addr}" }
        client.close rescue nil
      end
    end

    private def handle_tls_connection(ssl_client, local_addr, remote_addr)
      set_buffer_size(ssl_client)
      conn_info = ConnectionInfo.new(remote_addr, local_addr)
      conn_info.ssl = true
      conn_info.ssl_version = ssl_client.tls_version
      conn_info.ssl_cipher = ssl_client.cipher
      handle_connection(ssl_client, conn_info)
    end

    private def extract_conn_info(client) : ConnectionInfo
      remote_address = client.remote_address

      if @config.tcp_proxy_protocol?
        parsed_proxy = ProxyProtocol.parse(client)
        if trusted_proxy_source?(remote_address.address)
          return parsed_proxy if parsed_proxy
        else
          Log.warn { "PROXY protocol from untrusted source #{remote_address}, ignoring header" } if parsed_proxy
        end
      else
        if @config.clustering? && @server.all_followers.any? { |f| f.remote_address.address == remote_address.address }
          parsed_proxy = ProxyProtocol.parse(client)
          return parsed_proxy if parsed_proxy
        end
      end
      ConnectionInfo.new(remote_address, client.local_address)
    end

    private def trusted_proxy_source?(address : String) : Bool
      # If no trusted sources are configured, accept from all sources for backward compatibility
      return true if @config.proxy_protocol_trusted_sources.empty?
      @config.proxy_protocol_trusted_sources.any?(&.matches?(address))
    end

    private def set_socket_options(socket)
      unless socket.remote_address.loopback?
        if keepalive = @config.tcp_keepalive
          socket.keepalive = true
          socket.tcp_keepalive_idle = keepalive[0]
          socket.tcp_keepalive_interval = keepalive[1]
          socket.tcp_keepalive_count = keepalive[2]
        end
      end
      socket.tcp_nodelay = true if @config.tcp_nodelay?
      @config.tcp_recv_buffer_size.try { |v| socket.recv_buffer_size = v }
      @config.tcp_send_buffer_size.try { |v| socket.send_buffer_size = v }
    end

    private def set_buffer_size(socket)
      if @config.socket_buffer_size.positive?
        socket.buffer_size = @config.socket_buffer_size
        socket.sync = false
        socket.read_buffering = true
      else
        socket.sync = true
        socket.read_buffering = false
      end
    end
  end
end
