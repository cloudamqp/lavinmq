require "log"
require "file"
require "systemd"
require "./server"
require "./amqp/server"
require "./mqtt/server"
require "./http/http_server"
require "./http/metrics_server"
require "./data_dir_lock"
require "./pidfile"
require "./etcd"
require "./clustering/controller"
require "./clustering/etcd_coordinator"
require "./raft/coordinator"
require "./raft/runner"
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
    @server : LavinMQ::Server?
    @amqp_server : LavinMQ::AMQP::Server?
    @mqtt_server : LavinMQ::MQTT::Server?

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
        case @config.clustering_backend
        when "etcd"
          etcd = Etcd.new(@config.clustering_etcd_endpoints)
          coordinator = Clustering::EtcdCoordinator.new(@config, etcd)
          @runner = controller = Clustering::Controller.new(@config, etcd, coordinator)
          @replicator = Clustering::Server.new(@config, coordinator, controller.id)
        when "raft"
          @runner = runner = LavinMQ::Raft::Runner.new(@config)
          @replicator = Clustering::Server.new(@config, runner.coordinator, runner.node_id)
        else
          raise LavinMQ::Error.new("Invalid clustering_backend: #{@config.clustering_backend.inspect}")
        end
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
      @server = server = LavinMQ::Server.new(@config, @replicator)
      load_definitions(server)
      server.start_log_exchange
      @amqp_server = amqp_server = LavinMQ::AMQP::Server.new(server, @config)
      @mqtt_server = mqtt_server = LavinMQ::MQTT::Server.new(server, @config)
      @http_server = http_server = LavinMQ::HTTP::Server.new(server, amqp_server, mqtt_server, @runner)
      start_listeners(amqp_server, mqtt_server, http_server)
      start_metrics_server(server) unless @config.metrics_http_port == -1
      SystemD.notify_ready
      Fiber.yield # Yield to let listeners spawn before logging startup time
      Log.info { "Finished startup in #{(Time.instant - started_at).total_seconds}s" }
      self
    rescue ex : Socket::BindError
      stop
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
      @mqtt_server.try &.close rescue nil
      @server.try &.close rescue nil
      @metrics_server.try &.close rescue nil
      @runner.stop
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

    private def start_metrics_server(server)
      @metrics_server = metrics_server = LavinMQ::HTTP::MetricsServer.new(server)
      metrics_server.bind_tcp(@config.metrics_http_bind, @config.metrics_http_port)
      spawn(name: "HTTP metrics listener") do
        metrics_server.listen
      end
    end

    private def start_listeners(amqp_server, mqtt_server, http_server)
      bind_listeners(amqp_server, @config.amqp_bind, @config.amqp_port, @config.amqps_port, @amqp_tls_context, @config.unix_path)
      bind_listeners(mqtt_server, @config.mqtt_bind, @config.mqtt_port, @config.mqtts_port, @mqtt_tls_context, @config.mqtt_unix_path)
      bind_listeners(http_server, @config.http_bind, @config.http_port, @config.https_port, @http_tls_context, @config.http_unix_path)
      http_server.bind_internal_unix

      unless amqp_server.listeners.empty?
        spawn(name: "AMQP listener") do
          amqp_server.listen
        end
      end
      unless mqtt_server.listeners.empty?
        spawn(name: "MQTT listener") do
          mqtt_server.listen
        end
      end
      if clustering_bind = @config.clustering_bind
        if replicator = @replicator
          clustering_server = bind_clustering_listener(clustering_bind, @config.clustering_port)
          spawn(name: "Clustering listener") { replicator.listen(clustering_server) }
        end
      end
      spawn(name: "HTTP listener") do
        http_server.listen
      end
    end

    private def bind_clustering_listener(bind, port)
      TCPServer.new(bind, port)
    rescue ex : Socket::BindError
      abort "Error: #{ex.message}"
    end

    # Binds the plain TCP, TLS and unix listeners a server is configured for.
    # Works for any server exposing bind_tcp/bind_tls/bind_unix (the protocol
    # servers and the HTTP server).
    private def bind_listeners(server, bind, port, tls_port, tls_context, unix_path)
      server.bind_tcp(bind, port) if port > 0
      if tls_port > 0 && (ctx = tls_context)
        server.bind_tls(bind, tls_port, ctx)
      end
      server.bind_unix(unix_path) unless unix_path.empty?
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
        reload_config
      end
      SystemD.notify_ready
    end

    private def reload_config
      @config.reload
    rescue ex : Config::Error
      Log.warn { "Invalid configuration, keeping the running configuration: #{ex.message}" }
    else
      reload_tls
    end

    private def reload_tls
      reload_tls_context
    rescue ex
      Log.error { "Could not apply TLS changes on reload, a restart is required: #{ex.message}" }
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
      if @config.tls_configured? && @amqp_tls_context.nil?
        Log.warn { "Enabling TLS requires a restart to take effect" }
        return
      end
      if !@config.tls_configured? && @amqp_tls_context
        Log.warn { "Disabling TLS requires a restart to take effect" }
        return
      end
      {@amqp_tls_context, @mqtt_tls_context, @http_tls_context}.each do |ctx|
        next if ctx.nil?
        configure_tls_context(ctx)
      end
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
      # Set up SNI callback for AMQP TLS context
      if amqp_tls = @amqp_tls_context
        amqp_tls.on_server_name do |hostname|
          if sni_host = @config.sni_manager.get_host(hostname)
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
        mqtt_tls.on_server_name do |hostname|
          if sni_host = @config.sni_manager.get_host(hostname)
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
        http_tls.on_server_name do |hostname|
          if sni_host = @config.sni_manager.get_host(hostname)
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
