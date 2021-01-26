require "socket"
require "logger"
require "openssl"
require "systemd"
require "./amqp"
require "./rough_time"
require "../stdlib/*"
require "./vhost_store"
require "./user_store"
require "./exchange"
require "./queue"
require "./parameter"
require "./chained_logger"
require "./config"
require "./proxy_protocol"
require "./client/client"
require "./stats"
require "./event_type"

module AvalancheMQ
  class Server
    getter vhosts, users, data_dir, log, parameters
    getter? closed, flow
    alias Event = Channel(EventType)
    include ParameterTarget
    include Stats
    getter channel_closed_log, channel_created_log, connection_closed_log, connection_created_log,
      queue_declared_log, queue_deleted_log

    @start = Time.monotonic
    @closed = false
    @flow = true
    rate_stats(%w(channel_closed channel_created connection_closed connection_created
      queue_declared queue_deleted ack deliver get publish redeliver reject))

    def initialize(@data_dir : String, @log : Logger)
      @log.progname = "amqpserver"
      Dir.mkdir_p @data_dir
      @listeners = Array(Socket::Server).new(3)
      @users = UserStore.instance(@data_dir, @log)
      @events = Event.new(4096)
      spawn events_loop, name: "Server#events"
      @vhosts = VHostStore.new(@data_dir, @log, @users, @events)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @log)
      apply_parameter
      spawn stats_loop, name: "Server#stats_loop"
    end

    def connections
      Iterator(Client).chain(@vhosts.each_value.map(&.connections.each))
    end

    def vhost_connections(vhost_name)
      @vhosts[vhost_name].connections
    end

    def listen(s : TCPServer)
      @listeners << s
      @log.info { "Listening on #{s.local_address}" }
      loop do
        client = s.accept? || break
        client.sync = false
        client.read_buffering = true
        set_socket_options(client)
        if Config.instance.tcp_proxy_protocol
          spawn(handle_proxied_connection(client), name: "Server#handle_proxied_connection(tcp)")
        else
          spawn(handle_connection(client, client.remote_address, client.local_address), name: "Server#handle_connection(tcp)")
        end
      end
    rescue ex : IO::Error
      abort "Unrecoverable error in listener: #{ex.inspect}"
    ensure
      @listeners.delete(s)
    end

    def listen(s : UNIXServer)
      @listeners << s
      @log.info { "Listening on #{s.local_address}" }
      src = Socket::IPAddress.new("127.0.0.1", 0)
      dst = Socket::IPAddress.new("127.0.0.1", 0)
      while client = s.accept?
        client.sync = false
        client.read_buffering = true
        client.buffer_size = Config.instance.socket_buffer_size
        if Config.instance.unix_proxy_protocol
          spawn(handle_proxied_connection(client), name: "Server#handle_proxied_connection(unix)")
        else
          spawn(handle_connection(client, src, dst), name: "Server#handle_connection(unix)")
        end
      end
    rescue ex : IO::Error
      abort "Unrecoverable error in unix listener: #{ex.inspect}"
    ensure
      @listeners.delete(s)
    end

    def listen(bind = "::", port = 5672)
      s = TCPServer.new(bind, port)
      listen(s)
    end

    def listen_tls(bind, port, context)
      s = TCPServer.new(bind, port)
      @listeners << s
      @log.info { "Listening on #{s.local_address} (TLS)" }
      loop do
        begin
          client = s.accept? || break
          remote_addr = client.remote_address
          ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
          @log.info { "Connected #{ssl_client.tls_version} #{ssl_client.cipher}" }
          client.sync = true
          client.read_buffering = false
          # only do buffering on the tls socket
          ssl_client.sync = false
          ssl_client.read_buffering = true
          set_socket_options(client)
          spawn handle_connection(ssl_client, remote_addr, client.local_address), name: "Server#handle_connection(tls)"
        rescue ex
          @log.error "Error accepting TLS connection from #{remote_addr}: #{ex.inspect}"
          begin
            client.try &.close
          rescue ex2
            @log.error "Error closing socket: #{ex2.inspect}"
          end
        end
      end
    rescue ex : IO::Error | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.inspect}"
    ensure
      @listeners.delete(s)
    end

    def listen_unix(path : String)
      File.delete(path) if File.exists?(path)
      s = UNIXServer.new(path)
      File.chmod(path, 0o777)
      listen(s)
    end

    def close
      @closed = true
      @log.debug "Closing listeners"
      @listeners.each &.close
      @log.debug "Closing vhosts"
      @vhosts.close
      @log.debug "Closing #events channel"
      @events.close
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
            "path":     addr.path,
            "protocol": "amqp",
          }
        when TCPServer
          addr = l.local_address
          {
            "ip_address": addr.address,
            "protocol":   "amqp",
            "port":       addr.port,
          }
        else raise "Unexpected listener '#{l.class}'"
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
      client = Client.start(socket, remote_address, local_address, @vhosts, @users, @log, @events)
      if client.nil?
        socket.close
        @log.info { "Connection failed for remote_address=#{remote_address}" }
      end
    rescue ex : IO::Error | OpenSSL::SSL::Error
      @log.debug { "HandleConnection exception: #{ex.inspect}" }
      begin
        socket.close
      rescue
        nil
      end
    end

    private def handle_proxied_connection(client)
      proxyheader = ProxyProtocol::V1.parse(client)
      handle_connection(client, proxyheader.src, proxyheader.dst)
    rescue ex
      @log.info { "Error accepting proxied socket: #{ex.inspect}" }
      client.close rescue nil
    end

    private def set_socket_options(socket)
      unless socket.remote_address.loopback?
        socket.keepalive = true
        socket.tcp_keepalive_idle = 60
        socket.tcp_keepalive_count = 3
        socket.tcp_keepalive_interval = 10
      end
      socket.tcp_nodelay = Config.instance.tcp_nodelay
      socket.buffer_size = Config.instance.socket_buffer_size
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def events_loop
      while type = @events.receive?
        case type
        in EventType::ChannelClosed     then @channel_closed_count += 1
        in EventType::ChannelCreated    then @channel_created_count += 1
        in EventType::ConnectionClosed  then @connection_closed_count += 1
        in EventType::ConnectionCreated then @connection_created_count += 1
        in EventType::QueueDeclared     then @queue_declared_count += 1
        in EventType::QueueDeleted      then @queue_deleted_count += 1
        in EventType::ClientAck         then @ack_count += 1
        in EventType::ClientDeliver     then @deliver_count += 1
        in EventType::ClientGet         then @get_count += 1
        in EventType::ClientPublish     then @publish_count += 1
        in EventType::ClientRedeliver   then @redeliver_count += 1
        in EventType::ClientReject      then @reject_count += 1
        end
      end
    end

    def update_stats_rates
      @vhosts.each_value do |vhost|
        vhost.queues.each_value(&.update_rates)
        vhost.exchanges.each_value(&.update_rates)
        vhost.connections.each do |connection|
          connection.update_rates
          connection.channels.each_value(&.update_rates)
        end
      end
      update_rates()
    end

    private def stats_loop
      statm = File.open("/proc/self/statm") if File.exists?("/proc/self/statm")
      loop do
        break if closed?
        sleep Config.instance.stats_interval.milliseconds
        update_stats_rates

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

        until @rss_log.size < log_size
          @rss_log.shift
        end

        rss = 0i64
        if statm
          statm.rewind
          output = statm.gets_to_end
          if idx = output.index(' ')
            idx += 1
            if idx2 = output[idx..].index(' ')
              idx2 += idx - 1
              rss = output[idx..idx2].to_i64 * LibC.getpagesize
            end
          end
        else
          rss = `ps -o rss= -p $PPID`.to_i64? || 0
        end

        @rss_log.push rss
        @rss = rss

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
    ensure
      statm.try &.close
    end

    METRICS = {:user_time, :sys_time, :blocks_out, :blocks_in}

    {% for m in METRICS %}
      getter {{m.id}} = 0_i64
      getter {{m.id}}_log = Deque(Float64).new(Config.instance.stats_log_size)
    {% end %}
    getter rss = 0_i64
    getter rss_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_total = 0_i64
    getter disk_total_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_free = 0_i64
    getter disk_free_log = Deque(Int64).new(Config.instance.stats_log_size)

    SERVER_METRICS = {:connection_created, :connection_closed, :channel_created, :channel_closed,
                      :queue_declared, :queue_deleted}
    {% for sm in SERVER_METRICS %}
      getter {{sm.id}}_count = 0_u64
      getter {{sm.id}}_rate = 0_f64
    {% end %}

    private def control_flow!
      if @disk_free < 2_i64 * Config.instance.segment_size
        if flow?
          @log.info { "Low disk space: #{@disk_free.humanize}B, stopping flow" }
          flow(false)
        end
      elsif !flow?
        @log.info { "Not low on disk space, starting flow" }
        flow(true)
      elsif @disk_free < 3_i64 * Config.instance.segment_size
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
