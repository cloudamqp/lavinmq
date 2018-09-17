require "socket"
require "logger"
require "openssl"
require "./amqp"
require "./client/network_client"
require "./vhost_store"
require "./user_store"
require "./exchange"
require "./queue"
require "./durable_queue"
require "./parameter"

module AvalancheMQ
  class Server
    getter connections, vhosts, users, data_dir, log, parameters, config
    alias ConfigValue = UInt16
    alias ConnectionsEvents = Channel::Buffered(Tuple(Client, Symbol))
    include ParameterTarget

    @running = false
    @config = {"heartbeat" => 60_u16} of String => ConfigValue

    def initialize(@data_dir : String, log_level, log_prefix_systemd_level = false,
                   config = Hash(String, ConfigValue).new)
      @log = Logger.new(STDOUT)
      @log.level = log_level
      @log.progname = "amqpserver"
      @log.formatter = Logger::Formatter.new do |severity, _datetime, progname, message, io|
        if log_prefix_systemd_level
          io << case severity
          when Logger::Severity::DEBUG then "<7>"
          when Logger::Severity::INFO  then "<6>"
          when Logger::Severity::WARN  then "<4>"
          when Logger::Severity::ERROR then "<3>"
          when Logger::Severity::FATAL then "<0>"
          else
          end
        end
        io << progname << ": " << message
      end
      Dir.mkdir_p @data_dir
      @listeners = Array(TCPServer).new(1)
      @connections = Array(Client).new
      @connection_events = ConnectionsEvents.new(16)
      @vhosts = VHostStore.new(@data_dir, @connection_events, @log)
      @users = UserStore.new(@data_dir, @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      apply_parameter
      @config.merge!(config)
      spawn handle_connection_events, name: "Server#handle_connection_events"
    end

    def listen(port = 5672)
      @running = true
      s = TCPServer.new("::", port)
      @listeners << s
      @log.info { "Listening on #{s.local_address}" }
      loop do
        if socket = s.accept?
          spawn handle_connection(socket), name: "Server#handle_connection"
        else
          break
        end
      end
    rescue ex : Errno
      abort "Unrecoverable error in listener: #{ex.to_s}"
      puts "Fibers:"
      Fiber.list { |f| puts f.inspect }
    ensure
      @listeners.delete(s)
    end

    def listen_tls(port, cert_path : String, key_path : String, ca_path : String? = nil)
      @running = true
      s = TCPServer.new("::", port)
      @listeners << s
      context = OpenSSL::SSL::Context::Server.new
      context.certificate_chain = cert_path
      context.private_key = key_path
      context.ca_certificates = ca_path if ca_path
      @log.info { "Listening on #{s.local_address} (TLS)" }
      loop do
        if client = s.accept?
          begin
            ssl_client = OpenSSL::SSL::Socket::Server.new(client, context)
            ssl_client.sync_close = true
            spawn handle_connection(client, ssl_client), name: "Server#handle_connection(tls)"
          rescue e : OpenSSL::SSL::Error
            @log.error "Error accepting OpenSSL connection from #{client.remote_address}: #{e.inspect}"
          end
        else
          break
        end
      end
    rescue ex : Errno | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.to_s}"
      puts "Fibers:"
      Fiber.list { |f| puts f.inspect }
    ensure
      @listeners.delete(s)
    end

    def close
      @log.debug "Closing listeners"
      @listeners.each &.close
      @log.debug "Closing connections"
      @connections.each &.close
      @log.debug "Closing vhosts"
      @vhosts.close
    end

    def closed?
      !@running
    end

    def add_parameter(p : Parameter)
      @parameters.create p
      apply_parameter(p)
    end

    def delete_parameter(component_name, parameter_name)
      @parameters.delete({component_name, parameter_name})
      @log.warn("No action when deleting parameter #{component_name}")
    end

    def listeners
      @listeners.map do |l|
        addr = l.local_address
        {
          "ip_address": addr.address,
          "port":       addr.port,
        }
      end
    end

    def stop_shovels
      @vhosts.each { |v| v.stop_shovels }
    end

    private def apply_parameter(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        @log.warn("No action when applying parameter #{p.component_name}")
      end
    end

    private def handle_connection(socket : TCPSocket, ssl_client : OpenSSL::SSL::Socket? = nil)
      socket.keepalive = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.tcp_nodelay = true
      socket.write_timeout = 15
      socket.recv_buffer_size = 131072
      # socket.send_buffer_size = 65536
      client = NetworkClient.start(socket, ssl_client, @config, @vhosts, @users, @log)
      if client
        @connection_events.send({client, :connected})
        client.on_close do |c|
          @connection_events.send({c, :disconnected})
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
