require "log"
require "file"
require "systemd"
require "./reporter"
require "./server"
require "./http/http_server"
require "./log_formatter"
require "./in_memory_backend"
require "./data_dir_lock"

module LavinMQ
  class Launcher
    Log = ::Log.for "launcher"
    @tls_context : OpenSSL::SSL::Context::Server?
    @first_shutdown_attempt = true
    @data_dir_lock : DataDirLock?

    def initialize(@config : LavinMQ::Config)
      reload_logger

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
        @data_dir_lock = DataDirLock.new(@config.data_dir).tap &.acquire
      end
      @amqp_server = LavinMQ::Server.new(@config.data_dir)
      @http_server = LavinMQ::HTTP::Server.new(@amqp_server)
      @tls_context = create_tls_context if @config.tls_configured?
      reload_tls_context
      setup_signal_traps
      setup_log_exchange
    end

    def run
      listen
      SystemD.notify_ready
      if lock = @data_dir_lock
        lock.poll
      else
        loop do
          GC.collect
          sleep 30
        end
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

    private def reload_logger
      log_file = (path = @config.log_file) ? File.open(path, "a") : STDOUT
      broadcast_backend = ::Log::BroadcastBackend.new
      backend = if ENV.has_key?("JOURNAL_STREAM")
                  ::Log::IOBackend.new(io: log_file, formatter: JournalLogFormat, dispatcher: ::Log::DirectDispatcher)
                else
                  ::Log::IOBackend.new(io: log_file, formatter: StdoutLogFormat, dispatcher: ::Log::DirectDispatcher)
                end

      broadcast_backend.append(backend, @config.log_level)

      in_memory_backend = ::Log::InMemoryBackend.instance
      broadcast_backend.append(in_memory_backend, @config.log_level)

      ::Log.setup(@config.log_level, broadcast_backend)
      # ::Log.setup do |c|
      #   c.bind "*", , backend
      # end
      target = (path = @config.log_file) ? path : "stdout"
      Log.info &.emit("logger settings", level: @config.log_level.to_s, target: target)
    end

    private def setup_log_exchange
      return unless @config.log_exchange?
      exchange_name = "amq.lavinmq.log"
      vhost = @amqp_server.vhosts["/"]
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

    private def listen
      if @config.amqp_port > 0
        spawn @amqp_server.listen(@config.amqp_bind, @config.amqp_port),
          name: "AMQP listening on #{@config.amqp_port}"
      end

      if @config.amqps_port > 0
        if ctx = @tls_context
          spawn @amqp_server.listen_tls(@config.amqp_bind, @config.amqps_port, ctx),
            name: "AMQPS listening on #{@config.amqps_port}"
        else
          Log.warn { "Certificate for AMQPS not @configured" }
        end
      end

      if replication_bind = @config.replication_bind
        spawn @amqp_server.listen_replication(replication_bind, @config.replication_port), name: "Replication listener"
      end

      unless @config.unix_path.empty?
        spawn @amqp_server.listen_unix(@config.unix_path), name: "AMQP listening at #{@config.unix_path}"
      end

      if @config.http_port > 0
        @http_server.bind_tcp(@config.http_bind, @config.http_port)
      end
      if @config.https_port > 0
        if ctx = @tls_context
          @http_server.bind_tls(@config.http_bind, @config.https_port, ctx)
        end
      end
      unless @config.http_unix_path.empty?
        @http_server.bind_unix(@config.http_unix_path)
      end

      @http_server.bind_internal_unix
      spawn(name: "HTTP listener") do
        @http_server.not_nil!.listen
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
      Fiber.list do |f|
        puts "name=#{f.name}\tcontext=#{f.execution_context.try &.name}"
      end
      # LavinMQ::Reporter.report(@amqp_server)
      STDOUT.flush
    end

    private def run_gc
      STDOUT.puts "Unmapping all segments"
      STDOUT.flush
      @amqp_server.vhosts.each_value do |vhost|
        vhost.queues.each_value do |q|
          msg_store = q.@msg_store
          msg_store.@segments.each_value &.unmap
          msg_store.@acks.each_value &.unmap
        end
      end
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
        @config.parse(@config.config_file)
        reload_logger
        reload_tls_context
      end
      SystemD.notify_ready
    end

    private def shutdown_server
      if @first_shutdown_attempt
        @first_shutdown_attempt = false
        SystemD.notify_stopping
        Log.info { "Shutting down gracefully..." }
        @http_server.close
        @amqp_server.close
        @data_dir_lock.try &.release
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
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_OLD + ":@SECLEVEL=1"
      when "1.1"
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_OLD + ":@SECLEVEL=1"
      when "1.2", ""
        tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_INTERMEDIATE
      when "1.3"
        tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
        tls.ciphers = OpenSSL::SSL::Context::CIPHERS_INTERMEDIATE
      else
        Log.warn { "Unrecognized @config value for tls_min_version: '#{@config.tls_min_version}'" }
      end
      tls.certificate_chain = @config.tls_cert_path
      tls.private_key = @config.tls_key_path.empty? ? @config.tls_cert_path : @config.tls_key_path
      tls.ciphers = @config.tls_ciphers unless @config.tls_ciphers.empty?
    end
  end
end
