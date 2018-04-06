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
      @vhosts = Hash(String, VHost).new
      Dir.mkdir_p @data_dir
      load_definitions
      create_vhost("/")
      create_vhost("default")
      create_vhost("bunny_testbed")
      spawn handle_connection_events, name: "Server#handle_connection_events"
    end

    def listen(port : Int)
      s = TCPServer.new("::", port)
      @listeners << s
      @log.info "Listening on #{s.local_address}"
      loop do
        if socket = s.accept?
          spawn handle_connection(socket)
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
      s = TCPServer.new("::", port)
      @listeners << s
      context = OpenSSL::SSL::Context::Server.new
      context.certificate_chain = cert_path
      context.private_key = key_path
      @log.info "Listening on #{s.local_address} (TLS)"
      loop do
        if client = s.accept?
          begin
            ssl_client = OpenSSL::SSL::Socket::Server.new(client, context)
            ssl_client.sync_close = true
            ssl_client.sync = false
            spawn handle_connection(client, ssl_client)
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
      @listeners.delete(s)
    end

    def close
      @log.debug "Closing listeners"
      @listeners.each &.close
      @log.debug "Closing connections"
      @connections.each &.close
      @log.debug "Closing vhosts"
      @vhosts.each_value &.close
      save_definitions
    end

    def to_json(json : JSON::Builder)
      {
        vhosts: @vhosts.values
      }.to_json(json)
    end

    def create_vhost(name)
      return if @vhosts.has_key?(name)
      @vhosts[name] = VHost.new(name, @data_dir, @log)
      save_definitions
    end

    def delete_vhost(name)
      @vhosts.delete(name).try &.delete
      save_definitions
    end

    private def load_definitions
      file = File.join(@data_dir, "definitions.json")
      if File.exists?(file)
        File.open(File.join(@data_dir, "definitions.json"), "r") do |f|
          data = JSON.parse(f)
          data.each do |k, v|
            case k
            when "vhosts"
              v.each do |vhost|
                name = vhost["name"].as_s
                @vhosts[name] = VHost.new(name, @data_dir, @log)
              end
            else
              @log.warn("Unknown definition property: #{k}")
            end
          end
        end
      end
      @log.debug("#{@vhosts.size} vhosts loaded")
    rescue e : Exception
      @log.error("Could not load definitions.json: #{e.inspect}")
    end

    private def save_definitions
      @log.debug "Saving server definitions"
      tmpfile = File.join(@data_dir, "definitions.json.tmp")
      File.open(tmpfile, "w") { |f| self.to_json(f) }
      File.rename tmpfile, File.join(@data_dir, "definitions.json")
    end

    private def handle_connection(socket : TCPSocket, ssl_client : OpenSSL::SSL::Socket? = nil)
      socket.sync = false
      socket.keepalive = true
      socket.tcp_nodelay = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.linger = nil
      socket.write_timeout = 15
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
