require "file"
require "systemd"

module AvalancheMQ
  class Launcher
    @tls_context : (OpenSSL::SSL::Context::Server|Nil)
    @first_shutdown_attempt = true
    @log : Logger
    @lock : (File|Nil)

    def initialize(@config : AvalancheMQ::Config)
      @log = create_logger
      print_environment_info
      maximize_fd_limit
      Dir.mkdir_p @config.data_dir
      @lock = acquire_lock if @config.data_dir_lock
      @amqp_server = AvalancheMQ::Server.new(@config.data_dir, @log.dup)
      @http_server = AvalancheMQ::HTTP::Server.new(@amqp_server, @log.dup)
      @tls_context = create_tls_context if @config.tls_configured?
      reload_tls_context
      setup_signal_traps
      listen
      SystemD.notify("READY=1\n")
      GC.collect
      if @lock
        hold_lock(@lock.not_nil!)
      else
        sleep
      end
    end

    private def print_environment_info
      @log.info AvalancheMQ::BUILD_INFO
      {% unless flag?(:release) %}
        @log.warn "Not built in release mode"
      {% end %}
      {% if flag?(:preview_mt) %}
        @log.info "Multithreading: #{ENV.fetch("CRYSTAL_WORKERS", "4")} threads"
      {% end %}
      @log.info "Pid: #{Process.pid}"
      @log.info "@config file: #{@config.config_file}" unless @config.config_file.empty?
      @log.info "Data directory: #{@config.data_dir}"
    end

    private def create_logger
      log_file = (path = @config.log_file) ? File.open(path, "a") : STDOUT
      log = Logger.new(log_file, level: @config.log_level)
      AvalancheMQ::LogFormatter.use(log)
      log
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
          @log.warn { "Certificate for AMQPS not @configured" }
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

    private def maximize_fd_limit
      _, fd_limit_max = System.file_descriptor_limit
      System.file_descriptor_limit = fd_limit_max
      fd_limit_current, _ = System.file_descriptor_limit
      @log.info "FD limit: #{fd_limit_current}"
      if fd_limit_current < 1025
        @log.warn "The file descriptor limit is very low, consider raising it."
        @log.warn "You need one for each connection and two for each durable queue, and some more."
      end
    end

    private def dump_debug_info
      STDOUT.puts System.resource_usage
      STDOUT.puts GC.prof_stats
      puts "Fibers:"
      Fiber.list { |f| puts f.inspect }
      AvalancheMQ::Reporter.report(@amqp_server)
      STDOUT.puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
      File.open(File.join(@amqp_server.data_dir, "string_pool.dump"), "w") do |f|
        STDOUT.puts "Dumping string pool to #{f.path}"
        AvalancheMQ::Reporter.dump_string_pool(f)
      end
      AvalancheMQ::Reporter.print_queue_segments(@amqp_server, STDOUT)
      STDOUT.flush
    end

    private def run_gc
      STDOUT.puts "Garbage collecting"
      STDOUT.flush
      GC.collect
    end

    private def reload_server
      SystemD.notify("RELOADING=1\n")
      if @config.config_file.empty?
        @log.info { "No configuration file to reload" }
      else
        @log.info { "Reloading configuration file '#{@config.config_file}'" }
        @config.parse(@config.config_file)
        reload_log
        reload_tls_context
      end
      SystemD.notify("READY=1\n")
    end

    private def shutdown_server
      if @first_shutdown_attempt
        @first_shutdown_attempt = false
        SystemD.notify("STOPPING=1\n")
        @log.info "Shutting down gracefully..."
        @amqp_server.close
        @http_server.try &.close
        @lock.try &.close
        @log.info "Fibers: "
        Fiber.list { |f| @log.info f.inspect }
        exit 0
      else
        @log.info "Fibers: "
        Fiber.list { |f| @log.info f.inspect }
        exit 1
      end
    end

    private def setup_signal_traps
      Signal::USR1.trap { dump_debug_info }
      Signal::USR2.trap { run_gc }
      Signal::HUP.trap  { reload_server }
      Signal::INT.trap  { shutdown_server }
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
        @log.info "Data directory locked by '#{lock.gets_to_end}'"
        @log.info "Waiting for file lock to be released"
        lock.flock_exclusive(blocking: true)
        @log.info "Lock acquired"
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
        @log.warn { "Unrecognized @config value for tls_min_version: '#{@config.tls_min_version}'" }
      end
      tls.certificate_chain = @config.tls_cert_path
      tls.private_key = @config.tls_key_path.empty? ? @config.tls_cert_path : @config.tls_key_path
      tls.ciphers = @config.tls_ciphers unless @config.tls_ciphers.empty?
    end

    private def reload_log
      new_level = @config.log_level || AvalancheMQ::Config::DEFAULT_LOG_LEVEL
      return if @log.level == new_level
      @log.info { "Log level changed from #{@log.level} to #{new_level}" }
      @log.level = @config.log_level.not_nil!
    end
  end
end
