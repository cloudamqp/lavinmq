require "log"
require "file"
require "systemd"
require "./reporter"
require "./server"
require "./http/http_server"
require "./in_memory_backend"
require "./data_dir_lock"
require "./etcd"
require "./clustering/controller"

module LavinMQ
  struct StandaloneRunner
    # The block will be yielded when the runner's prerequisites for a leader
    # to start are met. For the standalone runner, this is immediately.
    # The method is blocking.
    def run(&)
      yield
      loop do
        sleep 30.seconds
        GC.collect
      end
    end

    def stop
    end
  end

  class Launcher
    Log = LavinMQ::Log.for "launcher"
    @tls_context : OpenSSL::SSL::Context::Server?
    @first_shutdown_attempt = true
    @data_dir_lock : DataDirLock?
    @closed = false

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
        @replicator = Clustering::Server.new(@config, etcd, controller.id)
      else
        @replicator = Clustering::NoopServer.new
        @runner = StandaloneRunner.new
      end

      @tls_context = create_tls_context if @config.tls_configured?
      reload_tls_context
      setup_signal_traps
    end

    private def start : self
      started_at = Time.monotonic
      @data_dir_lock.try &.acquire
      @amqp_server = amqp_server = LavinMQ::Server.new(@config, @replicator)
      @http_server = http_server = LavinMQ::HTTP::Server.new(amqp_server)
      setup_log_exchange(amqp_server)
      start_listeners(amqp_server, http_server)
      SystemD.notify_ready
      Fiber.yield # Yield to let listeners spawn before logging startup time
      Log.info { "Finished startup in #{(Time.monotonic - started_at).total_seconds}s" }
      self
    end

    def run
      @runner.run do
        start
      end
      @replicator.close
      @data_dir_lock.try &.release
    end

    def stop
      return if @closed
      @closed = true
      Log.warn { "Stopping" }
      SystemD.notify_stopping
      @http_server.try &.close rescue nil
      @amqp_server.try &.close rescue nil
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
      Log.info { "Pid: #{Process.pid}" }
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

    private def setup_log_exchange(amqp_server)
      return unless @config.log_exchange?
      exchange_name = "amq.lavinmq.log"
      vhost = amqp_server.vhosts["/"]
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

    # ameba:disable Metrics/CyclomaticComplexity
    private def start_listeners(amqp_server, http_server)
      if @config.amqp_port > 0
        spawn amqp_server.listen(@config.amqp_bind, @config.amqp_port, Server::Protocol::AMQP),
          name: "AMQP listening on #{@config.amqp_port}"
      end

      if @config.amqps_port > 0
        if ctx = @tls_context
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
        if ctx = @tls_context
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
        if ctx = @tls_context
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
      LavinMQ::Reporter.report(@amqp_server)
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
        Fiber.yield
        Log.info { "Fibers: " }
        Fiber.list { |f| Log.info { f.inspect } }
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
      context = OpenSSL::SSL::Context::Server.new
      context.add_options(OpenSSL::SSL::Options.new(0x40000000)) # disable client initiated renegotiation
      context
    end

    private def reload_tls_context
      return unless tls = @tls_context
      case @config.tls_min_version
      when "1.0"
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                           OpenSSL::SSL::Options::NO_TLS_V1_1 |
                           OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.1"
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.2", ""
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
      when "1.3"
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
      else
        Log.warn { "Unrecognized @config value for tls_min_version: '#{@config.tls_min_version}'" }
      end
      tls.certificate_chain = @config.tls_cert_path
      tls.private_key = @config.tls_key_path.empty? ? @config.tls_cert_path : @config.tls_key_path
      tls.ciphers = @config.tls_ciphers unless @config.tls_ciphers.empty?
    end
  end
end
