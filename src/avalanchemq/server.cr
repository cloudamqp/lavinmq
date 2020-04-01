require "socket"
require "logger"
require "openssl"
require "./amqp"
require "./rough_time"
require "../stdlib/*"
require "./client/network_client"
require "./vhost_store"
require "./user_store"
require "./exchange"
require "./queue"
require "./durable_queue"
require "./parameter"
require "./chained_logger"
require "./config"
require "./proxy_protocol"

module AvalancheMQ
  class Server
    getter connections, vhosts, users, data_dir, log, parameters
    getter? closed, flow
    alias ConnectionsEvents = Channel(Tuple(Client, Symbol))
    include ParameterTarget

    @start = Time.monotonic
    @closed = false
    @flow = true

    def initialize(@data_dir : String, @log : Logger)
      @log.progname = "amqpserver"
      Dir.mkdir_p @data_dir
      @listeners = Array(Socket).new(3)
      @connections = Array(Client).new
      @connection_events = ConnectionsEvents.new(16)
      @users = UserStore.new(@data_dir, @log)
      @vhosts = VHostStore.new(@data_dir, @connection_events, @log, @users.default_user)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      apply_parameter
      spawn handle_connection_events, name: "Server#handle_connection_events"
      spawn stats_loop, name: "Server#stats_loop"
    end

    def listen(bind = "::", port = 5672)
      s = TCPServer.new(bind, port)
      @listeners << s
      @log.info { "Listening on #{s.local_address}" }
      loop do
        client = s.accept? || break
        client.sync = false
        client.read_buffering = true
        set_socket_options(client)
        spawn handle_connection(client, client.remote_address, client.local_address), name: "Server#handle_connection"
      end
    rescue ex : Errno
      abort "Unrecoverable error in listener: #{ex.inspect}"
    ensure
      @listeners.delete(s)
    end

    def listen_tls(bind, port, cert_path : String, key_path : String, ca_path : String? = nil)
      s = TCPServer.new(bind, port)
      @listeners << s
      context = OpenSSL::SSL::Context::Server.new
      context.certificate_chain = cert_path
      context.private_key = key_path
      context.ca_certificates = ca_path if ca_path
      # context.ciphers = "ECDHE-RSA-AES128-SHA256"
      @log.info { "Listening on #{s.local_address} (TLS)" }
      loop do
        begin
          client = s.accept? || break
          ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
          @log.info { "Connected #{ssl_client.try &.tls_version} #{ssl_client.try &.cipher}" }
          client.sync = true
          client.read_buffering = false
          # only do buffering on the tls socket
          ssl_client.sync = false
          ssl_client.read_buffering = true
          set_socket_options(client)
          spawn handle_connection(ssl_client, client.remote_address, client.local_address), name: "Server#handle_connection(tls)"
        rescue ex
          @log.error "Error accepting OpenSSL connection: #{ex.inspect}"
          begin
            client.try &.close
          rescue ex2
            @log.error "Error closing socket: #{ex2.inspect}"
          end
        end
      end
    rescue ex : Errno | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.inspect}"
    ensure
      @listeners.delete(s)
    end

    def listen_unix(path : String, proxy_protocol_version = 1)
      File.delete(path) if File.exists?(path)
      s = UNIXServer.new(path)
      @listeners << s
      File.chmod(path, 0o777)
      @log.info { "Listening on #{s.local_address}" }
      while client = s.accept?
        client.sync = false
        client.read_buffering = true
        client.write_timeout = 15
        client.buffer_size = Config.instance.socket_buffer_size
        proxyheader =
          begin
            case proxy_protocol_version
            when 0 then ProxyProtocol::Header.local
            when 1 then ProxyProtocol::V1.parse(client)
            else        raise "Unsupported proxy protocol version #{proxy_protocol_version}"
            end
          rescue ex
            @log.info { "Error accepting UNIX socket: #{ex.inspect}" }
            client.close
            next
          end
        spawn handle_connection(client, proxyheader.src, proxyheader.dst), name: "Server#handle_connection(unix)"
      end
    rescue ex : Errno
      abort "Unrecoverable error in unix listener: #{ex.inspect}"
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
    end

    def listeners
      @listeners.map do |l|
        case l
        when UNIXServer
          addr = l.local_address
          {
            "path": addr.path,
          }
        when TCPServer
          addr = l.local_address
          {
            "ip_address": addr.address,
            "port":       addr.port,
          }
        end
      end
    end

    def stop_shovels
      @log.info("Stopping shovels")
      @vhosts.each_value { |v| v.stop_shovels }
    end

    private def apply_parameter(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        @log.warn("No action when applying parameter #{p.parameter_name}")
      end
    end

    private def handle_connection(socket, remote_address, local_address)
      client = NetworkClient.start(socket, remote_address, local_address, @vhosts, @users, @log)
      if client
        @connection_events.send({client, :connected})
        client.on_close do |c|
          @connection_events.send({c, :disconnected})
        end
      else
        socket.close
      end
    rescue ex : Errno
      @log.debug { "HandleConnection exception: #{ex.inspect}" }
    end

    private def set_socket_options(socket)
      socket.keepalive = true
      socket.tcp_keepalive_idle = 60
      socket.tcp_keepalive_count = 3
      socket.tcp_keepalive_interval = 10
      socket.tcp_nodelay = false
      socket.write_timeout = 15
      socket.buffer_size = Config.instance.socket_buffer_size
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
        sleep Config.instance.stats_interval.milliseconds
        @vhosts.each_value do |vhost|
          vhost.queues.each_value(&.update_rates)
          vhost.exchanges.each_value(&.update_rates)
        end
        @connections.each do |connection|
          connection.update_rates
          connection.channels.each_value(&.update_rates)
        end

        interval = Config.instance.stats_interval.milliseconds.to_i
        log_size = Config.instance.stats_log_size
        rusage = System.resource_usage

        {% for m in METRICS %}
          until @{{m.id}}_log.size < log_size
            @{{m.id}}_log.shift
          end
          {% if m.id.ends_with? "_time" %}
            {{m.id}} = rusage.{{m.id}}.total_milliseconds.to_i64
            {{m.id}}_rate = (({{m.id}} - @{{m.id}}) / (interval * 1000)).round(2)
          {% else %}
            {{m.id}} = rusage.{{m.id}}.to_i64
            {{m.id}}_rate = (({{m.id}} - @{{m.id}}) / interval).round(2)
          {% end %}
          @{{m.id}}_log.push {{m.id}}_rate
          @{{m.id}} = {{m.id}}
        {% end %}

        until @max_rss_log.size < log_size
          @max_rss_log.shift
        end
        max_rss = rusage.max_rss.to_i64
        @max_rss_log.push max_rss
        @max_rss = max_rss

        fs_stats = Filesystem.info(@data_dir)
        until @disk_free_log.size < log_size
          @disk_free_log.shift
        end
        disk_free = fs_stats.available.to_i64
        @disk_free_log.push disk_free
        @disk_free = disk_free

        until @disk_total_log.size < log_size
          @disk_total_log.shift
        end
        disk_total = fs_stats.total.to_i64
        @disk_total_log.push disk_total
        @disk_total = disk_total

        control_flow!
      end
    end

    METRICS = { :user_time, :sys_time, :blocks_out, :blocks_in }

    {% for m in METRICS %}
      getter {{m.id}} = 0_i64
      getter {{m.id}}_log = Deque(Float64).new(Config.instance.stats_log_size)
    {% end %}
    getter max_rss = 0_i64
    getter max_rss_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_total = 0_i64
    getter disk_total_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_free = 0_i64
    getter disk_free_log = Deque(Int64).new(Config.instance.stats_log_size)

    private def control_flow!
      if @disk_free < Config.instance.segment_size * 2
        if flow?
          @log.info { "Low disk space: #{@disk_free.humanize}B, stopping flow" }
          flow(false)
        end
      elsif !flow?
        @log.info { "Not low on disk space, starting flow" }
        flow(true)
      elsif @disk_free < Config.instance.segment_size * 3
        @log.info { "Low on disk space: #{@disk_free.humanize}B" }
      end
    end

    def flow(active : Bool)
      @flow = active
      @vhosts.each_value { |v| v.flow = active }
    end

    def uptime
      Time.monotonic - @start
    end
  end
end
