require "socket"
require "logger"
require "openssl"
require "./amqp"
require "./stdlib_fixes"
require "./client/network_client"
require "./vhost_store"
require "./user_store"
require "./exchange"
require "./queue"
require "./durable_queue"
require "./parameter"
require "./chained_logger"
require "./config"
require "benchmark"

module AvalancheMQ
  class Server
    getter connections, vhosts, users, data_dir, log, parameters
    getter? closed
    alias ConnectionsEvents = Channel::Buffered(Tuple(Client, Symbol))
    include ParameterTarget

    @closed = false

    def self.config
      @@config.not_nil!
    end

    def initialize(@data_dir : String, @log : Logger, @@config = Config.new)
      @log.progname = "amqpserver"
      Dir.mkdir_p @data_dir
      @listeners = Array(TCPServer).new(1)
      @connections = Array(Client).new
      @connection_events = ConnectionsEvents.new(16)
      @vhosts = VHostStore.new(@data_dir, @connection_events, @log)
      @users = UserStore.new(@data_dir, @log)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      apply_parameter
      spawn handle_connection_events, name: "Server#handle_connection_events"
      spawn stats_loop, name: "Server#stats_loop"
    end

    def listen(port = 5672)
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
      @closed = true
      @log.debug "Closing listeners"
      @listeners.each &.close
      @log.debug "Closing connections"
      @connections.each &.close("Broker shutdown")
      @log.debug "Closing vhosts"
      @vhosts.close
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
      @log.info("Stopping shovels")
      @vhosts.each_value { |v| v.stop_shovels }
    end

    private def apply_parameter(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        @log.warn("No action when applying parameter #{p.component_name}")
      end
    end

    private def handle_connection(socket : TCPSocket, ssl_client : OpenSSL::SSL::Socket? = nil)
      socket.sync = false
      socket.keepalive = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.tcp_nodelay = true
      socket.write_timeout = 15
      socket.recv_buffer_size = 131072
      socket.send_buffer_size = 131072
      client = NetworkClient.start(socket, ssl_client, @vhosts, @users, @log)
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

    private def stats_loop
      loop do
        break if closed?
        sleep Server.config.stats_interval.milliseconds
        @vhosts.each_value do |vhost|
          vhost.queues.each_value(&.update_rates)
          vhost.exchanges.each_value(&.update_rates)
          connections.each(&.update_rates)
        end
      end
    end
  end
end
