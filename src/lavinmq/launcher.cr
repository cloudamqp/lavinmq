require "log"
require "file"
require "systemd"
require "./server"
require "./http/http_server"
require "./http/metrics_server"
require "./in_memory_backend"
require "./data_dir_lock"
require "./pidfile"
require "./etcd"
require "./clustering/controller"
require "./clustering/etcd_coordinator"
require "./amqp/relay_rejecter"
require "./mqtt/relay_rejecter"
require "./standalone_runner"
require "./definitions"
require "../stdlib/openssl_on_server_name"

module LavinMQ
  class Launcher
    Log = LavinMQ::Log.for "launcher"
    @amqp_tls_context : OpenSSL::SSL::Context::Server?
    @mqtt_tls_context : OpenSSL::SSL::Context::Server?
    @http_tls_context : OpenSSL::SSL::Context::Server?
    @first_shutdown_attempt = true
    @data_dir_lock : DataDirLock?
    @closed = false
    @replicator : Clustering::Server?
    # DR relay services (AMQP/MQTT reject listeners + barebones HTTP API),
    # started lazily when the node enters relay mode.
    @relay_servers = Array(TCPServer).new
    @relay_http_server : ::HTTP::Server?

    def initialize(@config : Config)
      print_environment_info
      print_max_map_count
      fd_limit = System.maximize_fd_limit
      Log.info { "FD limit: #{fd_limit}" }
      if fd_limit < 1025
        Log.warn { "The file descriptor limit is very low, consider raising it." }
        Log.warn { "You need one for each connection and two for each durable queue, and some more." }
      end
      Dir.mkdir_p @config.data_dir
      if @config.data_dir_lock?
        @data_dir_lock = DataDirLock.new(@config.data_dir)
      end

      if @config.clustering?
        etcd = Etcd.new(@config.clustering_etcd_endpoints)
        @runner = controller = Clustering::Controller.new(@config, etcd)
        coordinator = Clustering::EtcdCoordinator.new(@config, etcd)
        @replicator = replicator = Clustering::Server.new(@config, coordinator, controller.id)
        # In DR (relay) mode the controller drives this server itself (it listens
        # for the region's followers and is fed by the upstream region) instead
        # of the local message store, and never yields to start the amqp server.
        controller.replicator = replicator
        # On a role flip, gracefully stop before the supervisor restarts us into
        # the re-derived role (instead of an abrupt exit 38).
        controller.on_role_change = -> { graceful_restart_for_role_change }
        # In relay mode the node serves no clients: reject AMQP/MQTT connections
        # and run only a barebones HTTP API (metrics + health + DR control).
        controller.on_relay_start = -> { start_relay_services(controller) }
      else
        @runner = StandaloneRunner.new
      end

      if @config.tls_configured?
        @amqp_tls_context = create_tls_context
        @mqtt_tls_context = create_tls_context
        @http_tls_context = create_tls_context
        warn_if_ktls_unavailable if @config.tls_ktls?
      end
      setup_sni_callbacks
      setup_signal_traps
      SystemD::MemoryPressure.monitor { GC.collect }
    end

    private def start : self
      started_at = Time.instant
      @data_dir_lock.try &.acquire
      @amqp_server = amqp_server = LavinMQ::Server.new(@config, @replicator)
      @http_server = http_server = LavinMQ::HTTP::Server.new(amqp_server)
      load_definitions(amqp_server)
      setup_log_exchange(amqp_server)
      start_listeners(amqp_server, http_server)
      start_metrics_server(amqp_server) unless @config.metrics_http_port == -1
      SystemD.notify_ready
      Fiber.yield # Yield to let listeners spawn before logging startup time
      Log.info { "Finished startup in #{(Time.instant - started_at).total_seconds}s" }
      self
    rescue ex : Socket::BindError
      abort "Error: #{ex.message}"
    end

    def run
      @runner.run do
        start
      end
      @replicator.try &.close
      @data_dir_lock.try &.release
    end

    def stop
      return if @closed
      @closed = true
      Log.warn { "Stopping" }
      SystemD.notify_stopping
      @http_server.try &.close rescue nil
      @amqp_server.try &.close rescue nil
      @metrics_server.try &.close rescue nil
      @relay_http_server.try &.close rescue nil
      @relay_servers.each &.close rescue nil
      @runner.stop
    end

    # The region's DR role flipped. Gracefully tear down this node's subsystems
    # (drains AMQP clients in primary mode; just closes the relay client in DR
    # mode) and exit 38 so the supervisor restarts into the re-derived role.
    private def graceful_restart_for_role_change : Nil
      Log.warn { "Region role changed; gracefully restarting to switch role" }
      stop
      Fiber.yield
      exit 38 # 38 for region role change; supervisor restarts into the new role
    end

    # A DR relay serves no application clients. Reject AMQP/MQTT connections with
    # a clear protocol-level signal, and expose a barebones HTTP API (Prometheus
    # metrics, a readiness /health, and the DR control routes) on the management
    # port instead of the full management server.
    private def start_relay_services(controller : Clustering::Controller) : Nil
      if @config.amqp_port > 0
        spawn_relay_reject_listener(@config.amqp_bind, @config.amqp_port, nil, "Relay AMQP rejecter") { |io| AMQP.reject_relay(io) }
      end
      if @config.amqps_port > 0 && (ctx = @amqp_tls_context)
        spawn_relay_reject_listener(@config.amqp_bind, @config.amqps_port, ctx, "Relay AMQPS rejecter") { |io| AMQP.reject_relay(io) }
      end
      if @config.mqtt_port > 0
        spawn_relay_reject_listener(@config.mqtt_bind, @config.mqtt_port, nil, "Relay MQTT rejecter") { |io| MQTT.reject_relay(io, @config.mqtt_max_packet_size) }
      end
      if @config.mqtts_port > 0 && (ctx = @mqtt_tls_context)
        spawn_relay_reject_listener(@config.mqtt_bind, @config.mqtts_port, ctx, "Relay MQTTS rejecter") { |io| MQTT.reject_relay(io, @config.mqtt_max_packet_size) }
      end

      http = @relay_http_server = LavinMQ::HTTP::Server.relay_api(-> { controller.relay_ready? })
      if @config.http_port > 0
        addr = http.bind_tcp(@config.http_bind, @config.http_port)
        Log.info { "Relay HTTP API listening on #{addr}" }
      end
      if @config.https_port > 0 && (ctx = @http_tls_context)
        http.bind_tls(@config.http_bind, @config.https_port, ctx)
      end
      spawn(name: "Relay HTTP API listener") { http.listen }
    end

    private def spawn_relay_reject_listener(bind : String, port : Int32,
                                            tls_ctx : OpenSSL::SSL::Context::Server?,
                                            name : String, &reject : ::IO -> Nil) : Nil
      server = TCPServer.new(bind, port)
      @relay_servers << server
      Log.info { "Relay #{name} listening on #{server.local_address}" }
      spawn(name: name) do
        while socket = server.accept?
          spawn do
            io = tls_ctx ? OpenSSL::SSL::Socket::Server.new(socket, tls_ctx, sync_close: true).as(::IO) : socket.as(::IO)
            reject.call(io)
          rescue ex
            Log.warn(exception: ex) { "#{name} failed to reject connection" }
            socket.close rescue nil
          end
        end
      rescue IO::Error
        # listener closed during shutdown
      end
    end

    private def print_environment_info
      LavinMQ::BUILD_INFO.each_line do |line|
        Log.info { line }
      end
      {% unless flag?(:release) %}
        Log.warn { "Not built in release mode" }
      {% end %}
      {% if flag?(:preview_mt) %}
        Log.info { "Multithreading: #{ENV.fetch("CRYSTAL_WORKERS", "4")} threads" }
      {% end %}
      Log.info { "PID: #{Process.pid}" }
      # we do this here to have nice consistent logging
      Pidfile.new(@config.pidfile).acquire unless @config.pidfile.empty?
      Log.info { "Config file: #{@config.config_file}" } unless @config.config_file.empty?
      Log.info { "Data directory: #{@config.data_dir}" }
    end

    private def print_max_map_count
      {% if flag?(:linux) %}
        max_map_count = File.read("/proc/sys/vm/max_map_count").to_i
        Log.info { "Max mmap count: #{max_map_count}" }
        if max_map_count < 100_000
          Log.warn { "The max mmap count limit is very low, consider raising it." }
          Log.warn { "The limits should be higher than the maximum of # connections * 2 + # consumer * 2 + # queues * 4" }
          Log.warn { "sysctl -w vm.max_map_count=1000000" }
        end
      {% end %}
    end

    private def load_definitions(amqp_server)
      path = @config.load_definitions
      return if path.empty?
      GlobalDefinitions.import_from_file(path, amqp_server)
    rescue ex : File::NotFoundError
      Log.error { "Failed to load definitions: file '#{path}' does not exist" }
      exit 1
    rescue ex : File::AccessDeniedError
      Log.error { "Failed to load definitions: cannot read '#{path}': permission denied" }
      exit 1
    rescue ex : JSON::ParseException
      Log.error { "Failed to load definitions: invalid JSON in '#{path}': #{ex.message}" }
      exit 1
    rescue ex
      Log.error(exception: ex) { "Failed to load definitions from '#{path}'" }
      exit 1
    end

    private def setup_log_exchange(amqp_server)
      return unless @config.log_exchange?
      exchange_name = "amq.lavinmq.log"
      unless vhost = amqp_server.vhosts["/"]?
        Log.warn { "log_exchange enabled but default vhost \"/\" is missing, skipping" }
        return
      end
      vhost.declare_exchange(exchange_name, "topic", true, false, true)
      spawn(name: "Log Exchange") do
        log_channel = ::Log::InMemoryBackend.instance.add_channel
        while entry = log_channel.receive
          vhost.publish(msg: Message.new(
            exchange_name,
            entry.severity.to_s,
            "#{entry.source} - #{entry.message}",
            AMQP::Properties.new(timestamp: entry.timestamp, content_type: "text/plain")
          ))
        end
      end
    end

    private def start_metrics_server(amqp_server)
      @metrics_server = metrics_server = LavinMQ::HTTP::MetricsServer.new(amqp_server)
      metrics_server.bind_tcp(@config.metrics_http_bind, @config.metrics_http_port)
      spawn(name: "HTTP metrics listener") do
        metrics_server.listen
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def start_listeners(amqp_server, http_server)
      if @config.amqp_port > 0
        spawn amqp_server.listen(@config.amqp_bind, @config.amqp_port, Server::Protocol::AMQP),
          name: "AMQP listening on #{@config.amqp_port}"
      end

      if @config.amqps_port > 0
        if ctx = @amqp_tls_context
          spawn amqp_server.listen_tls(@config.amqp_bind, @config.amqps_port, ctx, Server::Protocol::AMQP),
            name: "AMQPS listening on #{@config.amqps_port}"
        end
      end

      if clustering_bind = @config.clustering_bind
        spawn amqp_server.listen_clustering(clustering_bind, @config.clustering_port), name: "Clustering listener"
      end

      unless @config.unix_path.empty?
        spawn amqp_server.listen_unix(@config.unix_path, Server::Protocol::AMQP), name: "AMQP listening at #{@config.unix_path}"
      end

      if @config.http_port > 0
        http_server.bind_tcp(@config.http_bind, @config.http_port)
      end
      if @config.https_port > 0
        if ctx = @http_tls_context
          http_server.bind_tls(@config.http_bind, @config.https_port, ctx)
        end
      end
      unless @config.http_unix_path.empty?
        http_server.bind_unix(@config.http_unix_path)
      end

      http_server.bind_internal_unix
      spawn(name: "HTTP listener") do
        http_server.listen
      end

      if @config.mqtt_port > 0
        spawn amqp_server.listen(@config.mqtt_bind, @config.mqtt_port, Server::Protocol::MQTT),
          name: "MQTT listening on #{@config.mqtt_port}"
      end

      if @config.mqtts_port > 0
        if ctx = @mqtt_tls_context
          spawn amqp_server.listen_tls(@config.mqtt_bind, @config.mqtts_port, ctx, Server::Protocol::MQTT),
            name: "MQTTS listening on #{@config.mqtts_port}"
        end
      end
      unless @config.mqtt_unix_path.empty?
        spawn amqp_server.listen_unix(@config.mqtt_unix_path, Server::Protocol::MQTT), name: "MQTT listening at #{@config.unix_path}"
      end
    end

    private def dump_debug_info
      puts "GC"
      ps = GC.prof_stats
      puts "  heap size: #{ps.heap_size.humanize_bytes}"
      puts "  free bytes: #{ps.free_bytes.humanize_bytes}"
      puts "  unmapped bytes: #{ps.unmapped_bytes.humanize_bytes}"
      puts "  allocated since last GC: #{ps.bytes_since_gc.humanize_bytes}"
      puts "  allocated before last GC: #{ps.bytes_before_gc.humanize_bytes}"
      puts "  non garbage collectable bytes: #{ps.non_gc_bytes.humanize_bytes}"
      puts "  garbage collection cycle number: #{ps.gc_no}"
      puts "  marker threads: #{ps.markers_m1}"
      puts "  reclaimed bytes during last GC: #{ps.bytes_reclaimed_since_gc.humanize_bytes}"
      puts "  reclaimed bytes before last GC: #{ps.reclaimed_bytes_before_gc.humanize_bytes}"
      puts "Fibers:"
      Fiber.list { |f| puts f.inspect }
      STDOUT.flush
    end

    private def run_gc
      STDOUT.puts "Garbage collecting"
      STDOUT.flush
      GC.collect
    end

    private def reload_server
      SystemD.notify_reloading
      if @config.config_file.empty?
        Log.info { "No configuration file to reload" }
      else
        Log.info { "Reloading configuration file '#{@config.config_file}'" }
        @config.reload
        reload_tls_context
      end
      SystemD.notify_ready
    end

    private def shutdown_server
      if @first_shutdown_attempt
        @first_shutdown_attempt = false
        stop
        Log.info { "Fibers: " }
        Fiber.list { |f| Log.info { f.inspect } }
        Fiber.yield
        exit 0
      else
        Log.info { "Fibers: " }
        Fiber.list { |f| Log.info { f.inspect } }
        exit 1
      end
    end

    private def setup_signal_traps
      Signal::USR1.trap { dump_debug_info }
      Signal::USR2.trap { run_gc }
      Signal::HUP.trap { reload_server }
      Signal::INT.trap { shutdown_server }
      Signal::TERM.trap { shutdown_server }
      Signal::SEGV.reset # Let the OS generate a coredump
    end

    private def create_tls_context
      ctx = OpenSSL::SSL::Context::Server.new
      configure_tls_context(ctx)
      ctx
    end

    private def warn_if_ktls_unavailable
      {% if flag?(:linux) && compare_versions(LibSSL::OPENSSL_VERSION, "3.0.0") >= 0 %}
        unless File.exists?("/sys/module/tls")
          Log.warn { "Kernel TLS module not loaded, kTLS will not be available (run: modprobe tls)" }
        end
      {% end %}
    end

    private def reload_tls_context
      {@amqp_tls_context, @mqtt_tls_context, @http_tls_context}.each do |ctx|
        next if ctx.nil?
        configure_tls_context(ctx)
      end
      @config.sni_manager.reload
    end

    private def configure_tls_context(ctx : OpenSSL::SSL::Context::Server)
      case @config.tls_min_version
      when "1.0"
        ctx.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                           OpenSSL::SSL::Options::NO_TLS_V1_1 |
                           OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.1"
        ctx.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
        ctx.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.2", ""
        ctx.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        ctx.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.3"
        ctx.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
      else
        Log.warn { "Unrecognized @config value for tls_min_version: '#{@config.tls_min_version}'" }
      end
      ctx.certificate_chain = @config.tls_cert_path
      ctx.private_key = @config.tls_key_path.empty? ? @config.tls_cert_path : @config.tls_key_path
      ctx.ciphers = @config.tls_ciphers unless @config.tls_ciphers.empty?
      if @config.tls_ktls?
        {% if OpenSSL::SSL::Options.has_constant?(:ENABLE_KTLS) %}
          ctx.add_options(OpenSSL::SSL::Options::ENABLE_KTLS)
        {% end %}
      end
      reload_ssl_keylog(ctx)
    end

    private def reload_ssl_keylog(ctx)
      keylog_file = @config.tls_keylog_file
      keylog_file = ENV.fetch("SSLKEYLOGFILE", "") if keylog_file.empty?
      if keylog_file.empty?
        ctx.keylog_file = nil
      else
        ctx.keylog_file = keylog_file
        Log.info { "SSL keylog enabled, writing to #{keylog_file}" }
      end
    end

    private def setup_sni_callbacks
      return if @config.sni_manager.empty?

      # Set up SNI callback for AMQP TLS context
      if amqp_tls = @amqp_tls_context
        sni_manager = @config.sni_manager
        amqp_tls.on_server_name do |hostname|
          if sni_host = sni_manager.get_host(hostname)
            Log.debug { "SNI (AMQP): Using certificate for hostname '#{hostname}'" }
            sni_host.amqp_tls_context
          else
            Log.debug { "SNI (AMQP): No specific certificate for '#{hostname}', using default" }
            nil
          end
        end
      end

      # Set up SNI callback for MQTT TLS context
      if mqtt_tls = @mqtt_tls_context
        sni_manager = @config.sni_manager
        mqtt_tls.on_server_name do |hostname|
          if sni_host = sni_manager.get_host(hostname)
            Log.debug { "SNI (MQTT): Using certificate for hostname '#{hostname}'" }
            sni_host.mqtt_tls_context
          else
            Log.debug { "SNI (MQTT): No specific certificate for '#{hostname}', using default" }
            nil
          end
        end
      end

      # Set up SNI callback for HTTP TLS context
      if http_tls = @http_tls_context
        sni_manager = @config.sni_manager
        http_tls.on_server_name do |hostname|
          if sni_host = sni_manager.get_host(hostname)
            Log.debug { "SNI (HTTP): Using certificate for hostname '#{hostname}'" }
            sni_host.http_tls_context
          else
            Log.debug { "SNI (HTTP): No specific certificate for '#{hostname}', using default" }
            nil
          end
        end
      end
    end
  end
end
