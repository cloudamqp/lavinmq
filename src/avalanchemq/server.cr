require "socket"
require "logger"
require "openssl"
require "./amqp"
require "./client"
require "./vhost"
require "./exchange"
require "./queue"
require "./durable_queue"

module AvalancheMQ
  class Server
    getter connections, vhosts, data_dir

    def initialize(@data_dir : String, log_level)
      @log = Logger.new(STDOUT)
      @log.level = log_level
      @log.progname = "AMQPServer"
      @log.formatter = Logger::Formatter.new do |severity, datetime, progname, message, io|
        io << progname << ": " << message
      end
      @listeners = Array(TCPServer).new(1)
      @connections = Array(Client).new
      @connection_events = Channel(Tuple(Client, Symbol)).new(16)
      Dir.mkdir_p @data_dir
      @vhosts = {
        "default" => VHost.new("default", @data_dir, @log),
        "bunny_testbed" => VHost.new("bunny_testbed", @data_dir, @log)
      }
      spawn handle_connection_events, name: "Server#handle_connection_events"
    end

    def listen(port : Int)
      s = TCPServer.new("::", port)
      @listeners << s
      @log.info "Listening on #{s.local_address}"
      loop do
        if socket = s.accept?
          handle_connection(socket)
        else
          break
        end
      end
    rescue ex : Errno
      abort "Unrecoverable error in listener: #{ex.to_s}"
    ensure
      @listeners.delete(s)
    end

    def listen_tls(port, cert_path : String, key_path : String)
      socket = TCPServer.new("::", port)
      @listeners << socket
      context = OpenSSL::SSL::Context::Server.new
      context.certificate_chain = cert_path
      context.private_key = key_path
      @log.info "Listening on #{socket.local_address} (TLS)"
      loop do
        if client = socket.accept?
          begin
            ssl_client = OpenSSL::SSL::Socket::Server.new(client, context)
            ssl_client.sync_close = true
            ssl_client.sync = false
            handle_connection(client, ssl_client)
          rescue e : OpenSSL::SSL::Error
            @log.error "Error accepting OpenSSL connection from #{client.remote_address}: #{e.inspect}"
          end
        else
          break
        end
      end
    rescue ex : Errno | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.to_s}"
    ensure
      @listeners.delete(socket)
    end

    def close
      @log.debug "Closing listeners"
      @listeners.each &.close
      @log.debug "Closing connections"
      @connections.each &.close
      @log.debug "Closing vhosts"
      @vhosts.each_value &.close
    end

    private def handle_connection(socket : TCPSocket, ssl_client : OpenSSL::SSL::Socket? = nil)
      socket.sync = false
      socket.keepalive = true
      socket.tcp_nodelay = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.linger = nil
      # socket.write_timeout = 5
      client =
        if ssl_client
          Client.start(ssl_client, socket.remote_address, @vhosts, @log)
        else
          Client.start(socket, socket.remote_address, @vhosts, @log)
        end
      if client
        @connection_events.send({ client, :connected })
        client.on_close do |c|
          @connection_events.send({ c, :disconnected })
        end
      else
        ssl_client.close if ssl_client
        socket.close
      end
    end

    private def handle_connection_events
      loop do
        conn, event = @connection_events.receive
        case event
        when :connected
          @connections.push conn
        when :disconnected
          @connections.delete conn
        end
        @log.debug { "#{@connections.size} connected clients" }
      end
    rescue Channel::ClosedError
      @log.debug { "Connection events channel closed" }
    end
  end
end
