require "socket"
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
require "./config"
require "./connection_info"
require "./proxy_protocol"
require "./client/client"
require "./stats"

module LavinMQ
  class Server
    getter vhosts, users, data_dir, parameters
    getter? closed, flow
    include ParameterTarget

    @start = Time.monotonic
    @closed = false
    @flow = true

    def initialize(@data_dir : String)
      @log = Log.for "amqpserver"
      Dir.mkdir_p @data_dir
      @listeners = Hash(Socket::Server, Symbol).new # Socket => protocol
      @users = UserStore.instance(@data_dir)
      @vhosts = VHostStore.new(@data_dir, @users)
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
      @listeners[s] = :amqp
      @log.info { "Listening on #{s.local_address}" }
      loop do
        client = s.accept? || break
        set_socket_options(client)
        set_buffer_size(client)
        case Config.instance.tcp_proxy_protocol
        when 1
          spawn(handle_proxied_v1_connection(client), name: "Server#handle_proxied_v1_connection(tcp)")
        when 2
          spawn(handle_proxied_v2_connection(client), name: "Server#handle_proxied_v2_connection(tcp)")
        else
          conn_info = ConnectionInfo.new(client.remote_address, client.local_address)
          spawn(handle_connection(client, conn_info), name: "Server#handle_connection(tcp)")
        end
      rescue ex : IO::Error
        Log.warn(exception: ex) { "while accepting connection" }
        client.try &.close rescue nil
      end
    rescue ex : IO::Error
      abort "Unrecoverable error in listener: #{ex.inspect_with_backtrace}"
    ensure
      @listeners.delete(s)
    end

    def listen(s : UNIXServer)
      @listeners[s] = :amqp
      @log.info { "Listening on #{s.local_address}" }
      while client = s.accept?
        begin
          set_buffer_size(client)
          case Config.instance.unix_proxy_protocol
          when 1
            spawn(handle_proxied_v1_connection(client), name: "Server#handle_proxied_v1_connection(tcp)")
          when 2
            spawn(handle_proxied_v2_connection(client), name: "Server#handle_proxied_v2_connection(tcp)")
          else
            # TODO: use unix socket address, don't fake local
            conn_info = ConnectionInfo.local
            spawn(handle_connection(client, conn_info), name: "Server#handle_connection(tcp)")
          end
        rescue ex : IO::Error
          @log.warn(exception: ex) { "IO::Error while accepting connection" }
          client.try &.close rescue nil
        end
      end
    rescue ex : IO::Error
      abort "Unrecoverable error in unix listener: #{ex.inspect_with_backtrace}"
    ensure
      @listeners.delete(s)
    end

    def listen(bind = "::", port = 5672)
      s = TCPServer.new(bind, port)
      listen(s)
    end

    def listen_tls(bind, port, context)
      s = TCPServer.new(bind, port)
      @listeners[s] = :amqps
      @log.info { "Listening on #{s.local_address} (TLS)" }
      while client = s.accept?
        remote_addr = client.remote_address
        begin
          set_socket_options(client)
          ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
          @log.info { "Connected #{ssl_client.tls_version} #{ssl_client.cipher}" }
          set_buffer_size(ssl_client)
          conn_info = ConnectionInfo.new(remote_addr, client.local_address)
          conn_info.ssl = true
          conn_info.ssl_version = ssl_client.tls_version
          conn_info.ssl_cipher = ssl_client.cipher
          spawn handle_connection(ssl_client, conn_info), name: "Server#handle_connection(tls)"
        rescue ex
          @log.error(exception: ex) { "Error accepting TLS connection from #{remote_addr}" }
          begin
            client.try &.close
          rescue ex2
            @log.error(exception: ex2) { "Error closing socket" }
          end
        end
      end
    rescue ex : IO::Error | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.inspect_with_backtrace}"
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
      @log.debug { "Closing listeners" }
      @listeners.each_key &.close
      @log.debug { "Closing vhosts" }
      @vhosts.close
    end

    def add_parameter(parameter : Parameter)
      @parameters.create parameter
      apply_parameter(parameter)
    end

    def delete_parameter(component_name, parameter_name)
      @parameters.delete({component_name, parameter_name})
    end

    def listeners
      @listeners.map do |l, protocol|
        case l
        when UNIXServer
          addr = l.local_address
          {
            "path":     addr.path,
            "protocol": protocol,
          }
        when TCPServer
          addr = l.local_address
          {
            "ip_address": addr.address,
            "protocol":   protocol,
            "port":       addr.port,
          }
        else raise "Unexpected listener '#{l.class}'"
        end
      end
    end

    def stop_shovels
      @log.info { "Stopping shovels" }
      @vhosts.each_value &.stop_shovels
    end

    private def apply_parameter(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        @log.warn { "No action when applying parameter #{p.parameter_name}" }
      end
    end

    def handle_connection(socket, connection_info)
      client = Client.start(socket, connection_info, @vhosts, @users)
    ensure
      socket.close if client.nil?
    end

    private def handle_proxied_v1_connection(client)
      handle_connection(client, ProxyProtocol::V1.parse(client))
    rescue ex
      @log.info { "Error accepting proxied socket: #{ex.inspect_with_backtrace}" }
      client.close rescue nil
    end

    private def handle_proxied_v2_connection(client)
      handle_connection(client, ProxyProtocol::V2.parse(client))
    rescue ex
      @log.info { "Error accepting proxied socket: #{ex.inspect_with_backtrace}" }
      client.close rescue nil
    end

    private def set_socket_options(socket)
      unless socket.remote_address.loopback?
        if keepalive = Config.instance.tcp_keepalive
          socket.keepalive = true
          socket.tcp_keepalive_idle = keepalive[0]
          socket.tcp_keepalive_interval = keepalive[1]
          socket.tcp_keepalive_count = keepalive[2]
        end
      end
      socket.tcp_nodelay = true if Config.instance.tcp_nodelay
      Config.instance.tcp_recv_buffer_size.try { |v| socket.recv_buffer_size = v }
      Config.instance.tcp_send_buffer_size.try { |v| socket.send_buffer_size = v }
    end

    private def set_buffer_size(socket)
      if Config.instance.socket_buffer_size.positive?
        socket.buffer_size = Config.instance.socket_buffer_size
        socket.sync = false
        socket.read_buffering = true
      else
        socket.sync = true
        socket.read_buffering = false
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
        vhost.update_rates
      end
    end

    def system_metrics(statm)
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

      rss = statm_rss(statm) || ps_rss
      @rss = rss
      @rss_log.push @rss

      @mem_limit = cgroup_memory_max || System.physical_memory.to_i64

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

    private def stats_loop
      # statm holds rss value in linux
      if File.exists?("/proc/self/statm")
        statm = File.open("/proc/self/statm").tap &.read_buffering = false
      end
      loop do
        break if closed?
        sleep Config.instance.stats_interval.milliseconds
        @stats_collection_duration_seconds_total = Time.measure do
          @stats_rates_collection_duration_seconds = Time.measure do
            update_stats_rates
          end
          @stats_system_collection_duration_seconds = Time.measure do
            system_metrics(statm)
          end
        end
      end
    ensure
      statm.try &.close
    end

    PAGE_SIZE = LibC.getpagesize

    # statm: https://man7.org/linux/man-pages/man5/proc.5.html
    # the second number in the output is the estimated RSS in pages
    private def statm_rss(statm) : Int64?
      return unless statm
      statm.rewind
      output = statm.gets_to_end
      if idx = output.index(' ', offset: 1)
        idx += 1
        if idx2 = output.index(' ', offset: idx)
          idx2 -= 1
          return output[idx..idx2].to_i64 * PAGE_SIZE
        end
      end
      @log.warn { "Could not parse /proc/self/statm: #{output}" }
    end

    # used on non linux systems
    private def ps_rss
      (`ps -o rss= -p $PPID`.to_i64? || 0i64) * 1024
    end

    # Available memory might be limited by a cgroup
    private def cgroup_memory_max : Int64?
      cgroup = File.read("/proc/self/cgroup")[/0::(.*)\n/, 1]? if File.exists?("/proc/self/cgroup")
      cgroup ||= "/"
      # cgroup v2
      begin
        return File.read("/sys/fs/cgroup#{cgroup}/memory.max").to_i64?
      rescue File::NotFoundError
      end
      # cgroup v1
      {
        "/sys/fs/cgroup#{cgroup}/memory.limit_in_bytes",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
      }.each do |path|
        l = File.read(path)
        return nil if l == "9223372036854771712\n" # Max in cgroup v1
        return l.to_i64?
      rescue File::NotFoundError
      end
    end

    METRICS = {:user_time, :sys_time, :blocks_out, :blocks_in}

    {% for m in METRICS %}
      getter {{m.id}} = 0_i64
      getter {{m.id}}_log = Deque(Float64).new(Config.instance.stats_log_size)
    {% end %}
    getter mem_limit = 0_i64
    getter rss = 0_i64
    getter rss_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_total = 0_i64
    getter disk_total_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter disk_free = 0_i64
    getter disk_free_log = Deque(Int64).new(Config.instance.stats_log_size)
    getter stats_collection_duration_seconds_total = Time::Span.new
    getter stats_rates_collection_duration_seconds = Time::Span.new
    getter stats_system_collection_duration_seconds = Time::Span.new

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
      @vhosts.each_value &.flow=(active)
    end

    def uptime
      Time.monotonic - @start
    end
  end
end
