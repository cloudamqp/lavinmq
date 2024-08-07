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
require "./client/amqp_connection"
require "./stats"

module LavinMQ
  class Server
    getter vhosts, users, data_dir, parameters
    getter? closed, flow
    include ParameterTarget

    @start = Time.monotonic
    @closed = false
    @flow = true
    @listeners = Hash(Socket::Server, Symbol).new # Socket => protocol
    @replicator : Clustering::Replicator
    Log = ::Log.for "amqpserver"

    def initialize(@data_dir : String, @replicator = Clustering::NoopServer.new)
      Dir.mkdir_p @data_dir
      Schema.migrate(@data_dir, @replicator)
      @users = UserStore.new(@data_dir, @replicator)
      @vhosts = VHostStore.new(@data_dir, @users, @replicator)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @replicator)
      apply_parameter
      spawn stats_loop, name: "Server#stats_loop"
    end

    def followers
      @replicator.followers
    end

    def amqp_url
      addr = @listeners.each_key.select(TCPServer).first.local_address
      "amqp://#{addr}"
    end

    def stop
      return if @closed
      @closed = true
      @vhosts.each_value &.close
      @replicator.clear
      Fiber.yield
    end

    def restart
      stop
      Dir.mkdir_p @data_dir
      Schema.migrate(@data_dir, @replicator)
      @users = UserStore.new(@data_dir, @replicator)
      @vhosts = VHostStore.new(@data_dir, @users, @replicator)
      @parameters = ParameterStore(Parameter).new(@data_dir, "parameters.json", @replicator)
      apply_parameter
      @closed = false
      Fiber.yield
    end

    def connections
      Iterator(Client).chain(@vhosts.each_value.map(&.connections.each))
    end

    def listen(s : TCPServer)
      @listeners[s] = :amqp
      Log.info { "Listening on #{s.local_address}" }
      loop do
        client = s.accept? || break
        next client.close if @closed
        spawn(name: "Accept TCP socket") do
          remote_address = client.remote_address
          set_socket_options(client)
          set_buffer_size(client)
          conn_info = extract_conn_info(client)
          handle_connection(client, conn_info)
        rescue ex
          Log.warn(exception: ex) { "Error accepting connection from #{remote_address}" }
          client.close rescue nil
        end
      end
    rescue ex : IO::Error
      abort "Unrecoverable error in listener: #{ex.inspect_with_backtrace}"
    ensure
      @listeners.delete(s)
    end

    private def extract_conn_info(client) : ConnectionInfo
      remote_address = client.remote_address
      case Config.instance.tcp_proxy_protocol
      when 1 then ProxyProtocol::V1.parse(client)
      when 2 then ProxyProtocol::V2.parse(client)
      else
        if client.peek[0, 5] == "PROXY".to_slice &&
           followers.any? { |f| f.remote_address.address == remote_address.address }
          # Expect PROXY protocol header if remote address is a follower
          ProxyProtocol::V1.parse(client)
        elsif client.peek[0, 8] == ProxyProtocol::V2::Signature.to_slice[0, 8] &&
              followers.any? { |f| f.remote_address.address == remote_address.address }
          # Expect PROXY protocol header if remote address is a follower
          ProxyProtocol::V2.parse(client)
        else
          ConnectionInfo.new(remote_address, client.local_address)
        end
      end
    end

    def listen(s : UNIXServer)
      @listeners[s] = :amqp
      Log.info { "Listening on #{s.local_address}" }
      loop do # do not try to use while
        client = s.accept? || break
        next client.close if @closed
        spawn(name: "Accept UNIX socket") do
          remote_address = client.remote_address
          set_buffer_size(client)
          conn_info =
            case Config.instance.unix_proxy_protocol
            when 1 then ProxyProtocol::V1.parse(client)
            when 2 then ProxyProtocol::V2.parse(client)
            else        ConnectionInfo.local # TODO: use unix socket address, don't fake local
            end
          handle_connection(client, conn_info)
        rescue ex
          Log.warn(exception: ex) { "Error accepting connection from #{remote_address}" }
          client.close rescue nil
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

    def listen_tls(s : TCPServer, context)
      @listeners[s] = :amqps
      Log.info { "Listening on #{s.local_address} (TLS)" }
      loop do # do not try to use while
        client = s.accept? || break
        next client.close if @closed
        spawn(name: "Accept TLS socket") do
          remote_addr = client.remote_address
          set_socket_options(client)
          ssl_client = OpenSSL::SSL::Socket::Server.new(client, context, sync_close: true)
          set_buffer_size(ssl_client)
          Log.debug { "#{remote_addr} connected with #{ssl_client.tls_version} #{ssl_client.cipher}" }
          conn_info = ConnectionInfo.new(remote_addr, client.local_address)
          conn_info.ssl = true
          conn_info.ssl_version = ssl_client.tls_version
          conn_info.ssl_cipher = ssl_client.cipher
          handle_connection(ssl_client, conn_info)
        rescue ex
          Log.warn(exception: ex) { "Error accepting TLS connection from #{remote_addr}" }
          client.close rescue nil
        end
      end
    rescue ex : IO::Error | OpenSSL::Error
      abort "Unrecoverable error in TLS listener: #{ex.inspect_with_backtrace}"
    ensure
      @listeners.delete(s)
    end

    def listen_tls(bind, port, context)
      listen_tls(TCPServer.new(bind, port), context)
    end

    def listen_unix(path : String)
      File.delete?(path)
      s = UNIXServer.new(path)
      File.chmod(path, 0o666)
      listen(s)
    end

    def listen_clustering(bind, port)
      @replicator.listen(TCPServer.new(bind, port))
    end

    def listen_clustering(server : TCPServer)
      @replicator.listen(server)
    end

    def close
      @closed = true
      Log.debug { "Closing listeners" }
      @listeners.each_key &.close
      Log.debug { "Closing vhosts" }
      @vhosts.close
      @replicator.close
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

    private def apply_parameter(parameter : Parameter? = nil)
      @parameters.apply(parameter) do |p|
        Log.warn { "No action when applying parameter #{p.parameter_name}" }
      end
    end

    def handle_connection(socket, connection_info)
      client = AMQPConnection.start(socket, connection_info, @vhosts, @users)
    ensure
      socket.close if client.nil?
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
      socket.tcp_nodelay = true if Config.instance.tcp_nodelay?
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

    def update_system_metrics(statm)
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

      begin
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
      rescue File::NotFoundError
        # Ignore when server is closed and deleted already
      end
    end

    private def stats_loop
      # statm holds rss value in linux
      if File.exists?("/proc/self/statm")
        statm = File.open("/proc/self/statm").tap &.read_buffering = false
      end
      until closed?
        @stats_collection_duration_seconds_total = Time.measure do
          @stats_rates_collection_duration_seconds = Time.measure do
            update_stats_rates
          end
          @stats_system_collection_duration_seconds = Time.measure do
            update_system_metrics(statm)
          end
        end

        control_flow!
        sleep Config.instance.stats_interval.milliseconds
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
      Log.warn { "Could not parse /proc/self/statm: #{output}" }
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
      if disk_full?
        if flow?
          Log.info { "Low disk space: #{@disk_free.humanize}B, stopping flow" }
          flow(false)
        end
      elsif !flow?
        Log.info { "Not low on disk space, starting flow" }
        flow(true)
      elsif disk_usage_over_warning_level?
        Log.info { "Low on disk space: #{@disk_free.humanize}B" }
      end
    end

    def disk_full?
      @disk_free < 3_i64 * Config.instance.segment_size || @disk_free < Config.instance.free_disk_min
    end

    def disk_usage_over_warning_level?
      @disk_free < 6_i64 * Config.instance.segment_size || @disk_free < Config.instance.free_disk_warn
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
