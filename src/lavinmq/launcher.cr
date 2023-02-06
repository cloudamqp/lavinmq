require "log"
require "file"
require "systemd"
require "./reporter"
require "./server"
require "./http/http_server"
require "./log_formatter"
require "./in_memory_backend"

module LavinMQ
  class Launcher
    Log = ::Log.for "launcher"
    @tls_context : OpenSSL::SSL::Context::Server?
    @first_shutdown_attempt = true
    @lock_file : File?

    def initialize(@config : LavinMQ::Config)
      reload_logger

      print_environment_info
      print_max_map_count
      Launcher.maximize_fd_limit
      Dir.mkdir_p @config.data_dir
      @lock_file = acquire_lock if @config.data_dir_lock?
      @amqp_server = LavinMQ::Server.new(@config.data_dir)
      @http_server = LavinMQ::HTTP::Server.new(@amqp_server)
      @tls_context = create_tls_context if @config.tls_configured?
      reload_tls_context
      setup_signal_traps
    end

    def run
      listen
      SystemD.notify_ready
      hostname = System.hostname.to_slice
      loop do
        GC.collect
        sleep 10
        @lock_file.try &.write_at(hostname, 0)
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
          Log.warn { "sysctl -w vm.max_map_count = 1000000" }
        end
      {% end %}
    end

    private def reload_logger
      log_file = (path = @config.log_file) ? File.open(path, "a") : STDOUT
      broadcast_backend = ::Log::BroadcastBackend.new
      backend = if ENV.has_key?("JOURNAL_STREAM")
                  ::Log::IOBackend.new(io: log_file, formatter: JournalLogFormat)
                else
                  ::Log::IOBackend.new(io: log_file, formatter: StdoutLogFormat)
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

    def self.maximize_fd_limit
      _, fd_limit_max = System.file_descriptor_limit
      System.file_descriptor_limit = fd_limit_max
      fd_limit_current, _ = System.file_descriptor_limit
      Log.info { "FD limit: #{fd_limit_current}" }
      if fd_limit_current < 1025
        Log.warn { "The file descriptor limit is very low, consider raising it." }
        Log.warn { "You need one for each connection and two for each durable queue, and some more." }
      end
    end

    private def dump_debug_info
      STDOUT.puts System.resource_usage
      STDOUT.puts GC.prof_stats
      STDOUT.puts "Fibers:"
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
        @lock_file.try &.close
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
    end

    # Make sure that only one instance is using the data directory
    # Can work as a poor mans cluster where the master nodes aquires
    # a file lock on a shared file system like NFS
    private def acquire_lock : File
      lock = File.open(File.join(@config.data_dir, ".lock"), "w+")
      lock.sync = true
      lock.read_buffering = false
      begin
        lock.flock_exclusive(blocking: false)
      rescue
        Log.info { "Data directory locked by '#{lock.gets_to_end}'" }
        Log.info { "Waiting for file lock to be released" }
        lock.flock_exclusive(blocking: true)
        Log.info { "Lock acquired" }
      end
      lock.truncate
      lock.print System.hostname
      lock.fsync
      lock
    end

    # write to the lock file to detect lost lock
    # See "Lost locks" in `man 2 fcntl`
    private def hold_lock(lock)
      hostname = System.hostname.to_slice
      loop do
        sleep 30
        lock.write_at hostname, 0
      end
    rescue ex : IO::Error
      abort "ERROR: Lost lock! #{ex.inspect}"
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
