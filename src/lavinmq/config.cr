require "log"
require "uri"
require "option_parser"
require "ini"
require "./version"
require "./log_formatter"
require "./in_memory_backend"
require "./auth/password"
require "./sni_config"
require "./config/options"

module LavinMQ
  class Config
<<<<<<< HEAD
    include Options
=======
    DEFAULT_LOG_LEVEL     = ::Log::Severity::Info
    DEFAULT_PASSWORD_HASH = "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+" # Hash of 'guest'

    property data_dir : String = ENV.fetch("STATE_DIRECTORY", "/var/lib/lavinmq")
    property config_file = File.exists?(File.join(ENV.fetch("CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini")) ? File.join(ENV.fetch("CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini") : ""
    property pidfile : String = ""
    property log_file : String? = nil
    property log_level : ::Log::Severity = DEFAULT_LOG_LEVEL
    property amqp_bind = "127.0.0.1"
    property amqp_port = 5672
    property amqps_port = 5671
    property mqtt_bind = "127.0.0.1"
    property mqtt_port = 1883
    property mqtts_port = 8883
    property mqtt_unix_path = ""
    property unix_path = ""
    property unix_proxy_protocol = 1_u8 # PROXY protocol version on unix domain socket connections
    property tcp_proxy_protocol = 0_u8  # PROXY protocol version on amqp tcp connections
    property tls_cert_path = ""
    property tls_key_path = ""
    property tls_ciphers = ""
    property tls_min_version = ""
    property tls_keylog_file = ""
    property http_bind = "127.0.0.1"
    property http_port = 15672
    property https_port = 15671
    property http_unix_path = ""
    property metrics_http_bind = "127.0.0.1"
    property metrics_http_port = 15692
    property heartbeat = 300_u16                     # second
    property frame_max = 131_072_u32                 # bytes
    property channel_max = 2048_u16                  # number
    property stats_interval = 5000                   # millisecond
    property stats_log_size = 120                    # 10 mins at 5s interval
    property? set_timestamp = false                  # in message headers when receive
    property socket_buffer_size = 16384              # bytes
    property? tcp_nodelay = false                    # bool
    property segment_size : Int32 = 8 * 1024**2      # bytes
    property max_inflight_messages : UInt16 = 65_535 # mqtt messages
    property default_mqtt_vhost = "/"
    property? mqtt_permission_check_enabled : Bool = false
    property? raise_gc_warn : Bool = false
    property? data_dir_lock : Bool = true
    property tcp_keepalive : Tuple(Int32, Int32, Int32)? = {60, 10, 3} # idle, interval, probes/count
    property tcp_recv_buffer_size : Int32? = nil
    property tcp_send_buffer_size : Int32? = nil
    property? default_user_only_loopback : Bool = true
    property max_message_size = 128 * 1024**2
    property? log_exchange : Bool = false
    property free_disk_min : Int64 = 0  # bytes
    property free_disk_warn : Int64 = 0 # bytes
    property? clustering = false
    property clustering_etcd_prefix = "lavinmq"
    property clustering_etcd_endpoints = "localhost:2379"
    property clustering_advertised_uri : String? = nil
    property clustering_bind = "127.0.0.1"
    property clustering_port = 5679
    property clustering_max_unsynced_actions = 8192 # number of unsynced clustering actions
    property clustering_on_leader_elected = ""      # shell command to execute when elected leader
    property clustering_on_leader_lost = ""         # shell command to execute when losing leadership
    property max_deleted_definitions = 8192         # number of deleted queues, unbinds etc that compacts the definitions file
    property consumer_timeout : UInt64? = nil
    property consumer_timeout_loop_interval = 60 # seconds
    property default_consumer_prefetch = UInt16::MAX
    property yield_each_received_bytes = 131_072    # max number of bytes to read from a client connection without letting other tasks in the server do any work
    property yield_each_delivered_bytes = 1_048_576 # max number of bytes sent to a client without tending to other tasks in the server
    property auth_backends : Array(String) = ["local"]
    property default_user : String = ENV.fetch("LAVINMQ_DEFAULT_USER", "guest")
    property default_password : String = ENV.fetch("LAVINMQ_DEFAULT_PASSWORD", DEFAULT_PASSWORD_HASH) # Hashed password for default user
    property max_consumers_per_channel = 0
    property mqtt_max_packet_size = 268_435_455_u32 # bytes
    getter sni_manager : SNIManager = SNIManager.new
>>>>>>> main
    @@instance : Config = self.new
    getter sni_manager : SNIManager = SNIManager.new

    def self.instance : LavinMQ::Config
      @@instance
    end

    private def initialize
    end

<<<<<<< HEAD
    # Parse configuration from environment, command line arguments and configuration file.
    # Command line arguments take precedence over environment variables,
    # which take precedence over the configuration file.
    def parse(argv = ARGV)
      @config_file = File.exists?(
        File.join(ENV.fetch("LAVINMQ_CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini")) ? File.join(ENV.fetch("LAVINMQ_CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini") : ""
      parse_config_from_cli(argv)
      parse_ini(@config_file)
      parse_env()
      parse_cli(argv)
=======
    def parse
      parser = OptionParser.new do |p|
        p.banner = "Usage: #{PROGRAM_NAME} [arguments]"
        p.separator("\nOptions:")
        p.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| @config_file = v }
        p.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| @data_dir = v }
        p.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |v|
          level = ::Log::Severity.parse?(v.to_s)
          @log_level = level if level
        end
        p.on("-d", "--debug", "Verbose logging") { @log_level = ::Log::Severity::Debug }
        p.on("--default-user-only-loopback=BOOL", "Limit default user to only connect from loopback address") do |v|
          @default_user_only_loopback = {"true", "yes", "y", "1"}.includes? v.to_s
        end
        p.on("--guest-only-loopback=BOOL", "(Deprecated) Limit default user to only connect from loopback address") do |v|
          # TODO: guest-only-loopback was deprecated in 2.2.x, remove in 3.0
          STDERR.puts "WARNING: 'guest_only_loopback' is deprecated, use '--default-user-only-loopback' instead"
          @default_user_only_loopback = {"true", "yes", "y", "1"}.includes? v.to_s
        end
        p.on("--default-consumer-prefetch=NUMBER", "Default consumer prefetch (default: #{@default_consumer_prefetch})") do |v|
          @default_consumer_prefetch = v.to_u16
        end
        p.on("--default-user=USER", "Default user (default: #{@default_user})") do |v|
          @default_user = v
        end
        p.on("--default-password-hash=PASSWORD-HASH", "Hashed password for default user (default: '+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+' (guest))") do |v|
          @default_password = v
        end
        p.on("--default-password=PASSWORD-HASH", "(Deprecated) Hashed password for default user (default: '+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+' (guest))") do |v|
          STDERR.puts "WARNING: 'default-password' is deprecated, use '--default-password-hash' instead"
          @default_password = v
        end
        p.on("--pidfile=FILE", "Write the process ID to FILE on startup. The file is removed upon graceful shutdown.") { |v| @pidfile = v }
        p.on("--no-data-dir-lock", "Don't put a file lock in the data directory") { @data_dir_lock = false }
        p.on("--raise-gc-warn", "Raise on GC warnings (default: #{@raise_gc_warn})") { @raise_gc_warn = true }

        p.separator("\nBindings & TLS:")
        p.on("-b BIND", "--bind=BIND", "IP address that the AMQP, MQTT and HTTP servers will listen on (default: #{@amqp_bind})") do |v|
          @amqp_bind = v
          @http_bind = v
          @mqtt_bind = v
        end
        p.on("-p PORT", "--amqp-port=PORT", "AMQP port to listen on (default: #{@amqp_port})") do |v|
          @amqp_port = v.to_i
        end
        p.on("--amqps-port=PORT", "AMQPS port to listen on (default: #{@amqps_port})") do |v|
          @amqps_port = v.to_i
        end
        p.on("--amqp-bind=BIND", "IP address that the AMQP server will listen on (default: #{@amqp_bind})") do |v|
          @amqp_bind = v
        end
        p.on("--mqtt-port=PORT", "MQTT port to listen on (default: #{@mqtt_port})") do |v|
          @mqtt_port = v.to_i
        end
        p.on("--mqtts-port=PORT", "MQTTS port to listen on (default: #{@mqtts_port})") do |v|
          @mqtts_port = v.to_i
        end
        p.on("--mqtt-bind=BIND", "IP address that the MQTT server will listen on (default: #{@mqtt_bind})") do |v|
          @mqtt_bind = v
        end
        p.on("--http-port=PORT", "HTTP port to listen on (default: #{@http_port})") do |v|
          @http_port = v.to_i
        end
        p.on("--https-port=PORT", "HTTPS port to listen on (default: #{@https_port})") do |v|
          @https_port = v.to_i
        end
        p.on("--http-bind=BIND", "IP address that the HTTP server will listen on (default: #{@http_bind})") do |v|
          @http_bind = v
        end
        p.on("--amqp-unix-path=PATH", "AMQP UNIX path to listen to") do |v|
          @unix_path = v
        end
        p.on("--http-unix-path=PATH", "HTTP UNIX path to listen to") do |v|
          @http_unix_path = v
        end
        p.on("--mqtt-unix-path=PATH", "MQTT UNIX path to listen to") do |v|
          @mqtt_unix_path = v
        end
        p.on("--metrics-http-bind=BIND", "IP address that the Prometheus server will bind to (default: #{@metrics_http_bind})") do |v|
          @metrics_http_bind = v
        end
        p.on("--metrics-http-port=PORT", "HTTP port that prometheus will listen to (default: #{@metrics_http_port})") do |v|
          @metrics_http_port = v.to_i
        end
        p.on("--cert FILE", "TLS certificate (including chain)") { |v| @tls_cert_path = v }
        p.on("--key FILE", "Private key for the TLS certificate") { |v| @tls_key_path = v }
        p.on("--ciphers CIPHERS", "List of TLS ciphers to allow") { |v| @tls_ciphers = v }
        p.on("--tls-min-version=VERSION", "Mininum allowed TLS version (default: #{@tls_min_version})") { |v| @tls_min_version = v }

        p.separator("\nClustering:")
        p.on("--clustering", "Enable clustering") do
          @clustering = true
        end
        p.on("--clustering-advertised-uri=URI", "Advertised URI for the clustering server (default: tcp://#{System.hostname}:#{@clustering_port})") do |v|
          @clustering_advertised_uri = v
        end
        p.on("--clustering-etcd-prefix=KEY", "Key prefix used in etcd (default: #{@clustering_etcd_prefix})") do |v|
          @clustering_etcd_prefix = v
        end
        p.on("--clustering-port=PORT", "Listen for clustering followers on this port (default: #{@clustering_port})") do |v|
          @clustering_port = v.to_i
        end
        p.on("--clustering-bind=BIND", "Listen for clustering followers on this address (default: #{@clustering_bind})") do |v|
          @clustering_bind = v
        end
        p.on("--clustering-max-unsynced-actions=ACTIONS", "Maximum unsynced actions") do |v|
          @clustering_max_unsynced_actions = v.to_i
        end
        p.on("--clustering-etcd-endpoints=URIs", "Comma separeted host/port pairs (default: #{@clustering_etcd_endpoints})") do |v|
          @clustering_etcd_endpoints = v
        end
        p.on("--clustering-on-leader-elected=COMMAND", "Shell command to execute when elected leader") do |v|
          @clustering_on_leader_elected = v
        end
        p.on("--clustering-on-leader-lost=COMMAND", "Shell command to execute when losing leadership") do |v|
          @clustering_on_leader_lost = v
        end

        p.separator("\nMiscellaneous:")
        p.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
        p.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
        p.on("-h", "--help", "Show this help") { puts p; exit 0 }
        p.invalid_option { |arg| abort "Invalid argument: #{arg}" }
      end
      parser.parse(ARGV.dup) # only parse args to get config_file
      parse(@config_file)
      parser.parse(ARGV.dup) # then override any config_file parameters with the cmd line args
      if @data_dir.empty?
        STDERR.puts "No data directory specified"
        STDERR.puts parser
        exit 2
      end
      reload_logger
      verify_default_password
    rescue ex
      abort ex.message
>>>>>>> main
    end

    private def parse_config_from_cli(argv)
      parser = OptionParser.new
      parser.on("-c CONFIG", "--config=CONFIG", "Path to config file") do |val|
        @config_file = val
      end
      # Silence errors - this is just a pre-parse to extract the config file path.
      # Full argument validation happens later in parse_cli.
      parser.invalid_option { }
      parser.missing_option { }
      parser.parse(argv.dup)
    end

    private def parse_env
      {% for ivar in @type.instance_vars.select(&.annotation(EnvOpt)) %}
        {% env_name, transform = ivar.annotation(EnvOpt).args %}
        if v = ENV.fetch({{env_name}}, nil)
          @{{ivar}} = parse_value(v, {{transform || ivar.type}})
        end
      {% end %}
    end

    private def parse_cli(argv)
      parser = OptionParser.new
      parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
      {% begin %}
        sections = {
          options:    {description: "Options", options: Array(Option).new},
          bindings:   {description: "Bindings", options: Array(Option).new},
          tls:        {description: "TLS", options: Array(Option).new},
          clustering: {description: "Clustering", options: Array(Option).new},
        }
        # Build sections structure and populate with CLI options from annotated instance variables
        {% for ivar in @type.instance_vars.select(&.annotation(CliOpt)) %}
          {%
            cli_opt = ivar.annotation(CliOpt)
            # If annotation has exactly 3 args (short_flag, long_flag, description),
            # use ivar.type as value parser. Otherwise, last arg is a custom value parser.
            if cli_opt.args.size == 3
              parser_arg = cli_opt.args
              value_parser = ivar.type
            else
              *parser_arg, value_parser = cli_opt.args
            end
            section_id = cli_opt[:section] || "options"
          %}
          # Create Option object with CLI args and a block that parses and stores the value
          # when the option is encountered during command line parsing
          sections[:{{section_id}}][:options] << Option.new({{parser_arg.splat}}, {{cli_opt[:deprecated]}}) do |value|
            self.{{ivar.name.id}} = parse_value(value, {{value_parser}})
          end
        {% end %}
        sections.each do |_section_id, section|
          parser.separator "\n#{section[:description]}"
          section[:options].sort.each do |opt|
            opt.setup_parser(parser)
          end
        end
      {% end %}
      parser.separator "\nMiscellaneous"
      parser.on("-h", "--help", "Show this help") { puts parser; exit 0 }
      parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
      parser.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
      parser.parse(argv.dup)
    end

    private def parse_ini(file)
      return if file.empty?
      abort "Config could not be found" unless File.file?(file)
      ini = INI.parse(File.read(file))
      {% begin %}
      ini.each do |section, settings|
        case section
        {% for section in INI_SECTIONS %}
        when {{section}}
          parse_section({{section}}, settings)
        {% end %}
        when .starts_with?("sni:") then parse_sni(section[4..], settings)
        when "replication"
          abort("#{file}: [replication] is deprecated and replaced with [clustering], see the README for more information")
        else
<<<<<<< HEAD
          raise "Unknown configuration section: #{section}"
=======
          raise "Unrecognized config section: #{section}"
        end
      end
    end

    def reload
      @sni_manager.clear
      parse(@config_file)
      reload_logger
    end

    private def reload_logger
      log_file = (path = @log_file) ? File.open(path, "a") : STDOUT
      broadcast_backend = ::Log::BroadcastBackend.new
      backend = if journald_stream?
                  ::Log::IOBackend.new(io: log_file, formatter: JournalLogFormat)
                else
                  ::Log::IOBackend.new(io: log_file, formatter: StdoutLogFormat)
                end

      broadcast_backend.append(backend, @log_level)

      in_memory_backend = ::Log::InMemoryBackend.instance
      broadcast_backend.append(in_memory_backend, @log_level)

      ::Log.setup(@log_level, broadcast_backend)
      target = (path = @log_file) ? path : "stdout"
      Log.info &.emit("Logger settings", level: @log_level.to_s, target: target)
    end

    def journald_stream? : Bool
      return false unless journal_stream = ENV["JOURNAL_STREAM"]?
      return false if @log_file # If logging to a file, not using journald

      # JOURNAL_STREAM format is "device:inode"
      parts = journal_stream.split(':')
      return false unless parts.size == 2

      journal_dev = parts[0].to_u64?
      journal_ino = parts[1].to_u64?
      return false unless journal_dev && journal_ino

      # Get STDOUT's device and inode
      LibC.fstat(STDOUT.fd, out stat)
      stat.st_dev == journal_dev && stat.st_ino == journal_ino
    rescue
      false
    end

    def tls_configured?
      !@tls_cert_path.empty?
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_main(settings)
      settings.each do |config, v|
        case config
        when "data_dir"                  then @data_dir = v
        when "data_dir_lock"             then @data_dir_lock = true?(v)
        when "pidfile"                   then @pidfile = v
        when "log_level"                 then @log_level = ::Log::Severity.parse(v)
        when "log_file"                  then @log_file = v
        when "stats_interval"            then @stats_interval = v.to_i32
        when "stats_log_size"            then @stats_log_size = v.to_i32
        when "segment_size"              then @segment_size = v.to_i32
        when "set_timestamp"             then @set_timestamp = true?(v)
        when "socket_buffer_size"        then @socket_buffer_size = v.to_i32
        when "tcp_nodelay"               then @tcp_nodelay = true?(v)
        when "tcp_keepalive"             then @tcp_keepalive = tcp_keepalive?(v)
        when "tcp_recv_buffer_size"      then @tcp_recv_buffer_size = v.to_i32?
        when "tcp_send_buffer_size"      then @tcp_send_buffer_size = v.to_i32?
        when "tls_cert"                  then @tls_cert_path = v
        when "tls_key"                   then @tls_key_path = v
        when "tls_ciphers"               then @tls_ciphers = v
        when "tls_min_version"           then @tls_min_version = v
        when "tls_keylog_file"           then @tls_keylog_file = v
        when "log_exchange"              then @log_exchange = true?(v)
        when "free_disk_min"             then @free_disk_min = v.to_i64
        when "free_disk_warn"            then @free_disk_warn = v.to_i64
        when "max_deleted_definitions"   then @max_deleted_definitions = v.to_i
        when "consumer_timeout"          then @consumer_timeout = v.to_u64
        when "default_consumer_prefetch" then @default_consumer_prefetch = v.to_u16
        when "metrics_http_bind"         then @metrics_http_bind = v
        when "metrics_http_port"         then @metrics_http_port = v.to_i
        when "default_user"              then @default_user = v
        when "default_password_hash"     then @default_password = v
        when "default_password"
          STDERR.puts "WARNING: 'default_password' is deprecated, use 'default_password_hash' instead"
          @default_password = v
        when "default_user_only_loopback" then @default_user_only_loopback = true?(v)
        when "guest_only_loopback" # TODO: guest_only_loopback was deprecated in 2.2.x, remove in 3.0
          STDERR.puts "WARNING: 'guest_only_loopback' is deprecated, use 'default_user_only_loopback' instead"
          @default_user_only_loopback = true?(v)
        else
          STDERR.puts "WARNING: Unrecognized configuration 'main/#{config}'"
        end
      end
    end

    private def parse_clustering(settings)
      settings.each do |config, v|
        case config
        when "enabled"              then @clustering = true?(v)
        when "etcd_prefix"          then @clustering_etcd_prefix = v
        when "etcd_endpoints"       then @clustering_etcd_endpoints = v
        when "advertised_uri"       then @clustering_advertised_uri = v
        when "bind"                 then @clustering_bind = v
        when "port"                 then @clustering_port = v.to_i32
        when "max_unsynced_actions" then @clustering_max_unsynced_actions = v.to_i32
        when "on_leader_elected"    then @clustering_on_leader_elected = v
        when "on_leader_lost"       then @clustering_on_leader_lost = v
        else
          STDERR.puts "WARNING: Unrecognized configuration 'clustering/#{config}'"
        end
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_amqp(settings)
      settings.each do |config, v|
        case config
        when "bind"                      then @amqp_bind = v
        when "port"                      then @amqp_port = v.to_i32
        when "tls_port"                  then @amqps_port = v.to_i32
        when "tls_cert"                  then @tls_cert_path = v # backward compatibility
        when "tls_key"                   then @tls_key_path = v  # backward compatibility
        when "unix_path"                 then @unix_path = v
        when "heartbeat"                 then @heartbeat = v.to_u16
        when "frame_max"                 then @frame_max = v.to_u32
        when "channel_max"               then @channel_max = v.to_u16
        when "max_message_size"          then @max_message_size = v.to_i32
        when "unix_proxy_protocol"       then @unix_proxy_protocol = true?(v) ? 1u8 : v.to_u8? || 0u8
        when "tcp_proxy_protocol"        then @tcp_proxy_protocol = true?(v) ? 1u8 : v.to_u8? || 0u8
        when "set_timestamp"             then @set_timestamp = true?(v)
        when "consumer_timeout"          then @consumer_timeout = v.to_u64
        when "default_consumer_prefetch" then @default_consumer_prefetch = v.to_u16
        when "max_consumers_per_channel" then @max_consumers_per_channel = v.to_i
        else
          STDERR.puts "WARNING: Unrecognized configuration 'amqp/#{config}'"
        end
      end
    end

    private def parse_mqtt(settings)
      settings.each do |config, v|
        case config
        when "bind"                     then @mqtt_bind = v
        when "port"                     then @mqtt_port = v.to_i32
        when "tls_port"                 then @mqtts_port = v.to_i32
        when "unix_path"                then @mqtt_unix_path = v
        when "max_inflight_messages"    then @max_inflight_messages = v.to_u16
        when "default_vhost"            then @default_mqtt_vhost = v
        when "permission_check_enabled" then @mqtt_permission_check_enabled = true?(v)
        when "max_packet_size"          then @mqtt_max_packet_size = v.to_u32
        else
          STDERR.puts "WARNING: Unrecognized configuration 'mqtt/#{config}'"
        end
      end
    end

    private def parse_mgmt(settings)
      settings.each do |config, v|
        case config
        when "bind"      then @http_bind = v
        when "port"      then @http_port = v.to_i32
        when "tls_port"  then @https_port = v.to_i32
        when "tls_cert"  then @tls_cert_path = v # backward compatibility
        when "tls_key"   then @tls_key_path = v  # backward compatibility
        when "unix_path" then @http_unix_path = v
        else
          STDERR.puts "WARNING: Unrecognized configuration 'mgmt/#{config}'"
        end
      end
    end

    private def parse_experimental(settings)
      settings.each do |config, v|
        case config
        when "yield_each_delivered_bytes" then @yield_each_delivered_bytes = v.to_i32
        when "yield_each_received_bytes"  then @yield_each_received_bytes = v.to_i32
        else
          STDERR.puts "WARNING: Unrecognized configuration 'experimental/#{config}'"
>>>>>>> main
        end
      end
      {% end %}
    rescue ex : ::INI::ParseException
      abort "Failed to parse config file '#{file}'. " \
            "Error on line #{ex.line_number}, column #{ex.column_number}"
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_sni(hostname : String, settings)
      host = @sni_manager.get_host(hostname) || SNIHost.new(hostname)
      settings.each do |config, v|
        case config
        # Default TLS settings
        when "tls_cert"        then host.tls_cert = v
        when "tls_key"         then host.tls_key = v
        when "tls_min_version" then host.tls_min_version = v
        when "tls_ciphers"     then host.tls_ciphers = v
        when "tls_verify_peer" then host.tls_verify_peer = true?(v)
        when "tls_ca_cert"     then host.tls_ca_cert = v
        when "tls_keylog_file" then host.tls_keylog_file = v
          # AMQP-specific overrides
        when "amqp_tls_cert"        then host.amqp_tls_cert = v
        when "amqp_tls_key"         then host.amqp_tls_key = v
        when "amqp_tls_min_version" then host.amqp_tls_min_version = v
        when "amqp_tls_ciphers"     then host.amqp_tls_ciphers = v
        when "amqp_tls_verify_peer" then host.amqp_tls_verify_peer = true?(v)
        when "amqp_tls_ca_cert"     then host.amqp_tls_ca_cert = v
        when "amqp_tls_keylog_file" then host.amqp_tls_keylog_file = v
          # MQTT-specific overrides
        when "mqtt_tls_cert"        then host.mqtt_tls_cert = v
        when "mqtt_tls_key"         then host.mqtt_tls_key = v
        when "mqtt_tls_min_version" then host.mqtt_tls_min_version = v
        when "mqtt_tls_ciphers"     then host.mqtt_tls_ciphers = v
        when "mqtt_tls_verify_peer" then host.mqtt_tls_verify_peer = true?(v)
        when "mqtt_tls_ca_cert"     then host.mqtt_tls_ca_cert = v
        when "mqtt_tls_keylog_file" then host.mqtt_tls_keylog_file = v
          # HTTP-specific overrides
        when "http_tls_cert"        then host.http_tls_cert = v
        when "http_tls_key"         then host.http_tls_key = v
        when "http_tls_min_version" then host.http_tls_min_version = v
        when "http_tls_ciphers"     then host.http_tls_ciphers = v
        when "http_tls_verify_peer" then host.http_tls_verify_peer = true?(v)
        when "http_tls_ca_cert"     then host.http_tls_ca_cert = v
        when "http_tls_keylog_file" then host.http_tls_keylog_file = v
        else
          STDERR.puts "WARNING: Unrecognized configuration 'sni:#{hostname}/#{config}'"
        end
      end
      if host.tls_cert.empty?
        STDERR.puts "WARNING: SNI host '#{hostname}' missing required tls_cert"
      else
        @sni_manager.add_host(host)
      end
    end

    private macro parse_section(section, settings)
    {% begin %}
    {%
      ivars_in_section = @type.instance_vars
        # Filter out ivars for given section
        .reject(&.annotation(IniOpt).nil?)
        .select(&.annotation(IniOpt)[:section].== section)
        # This is just to get simpler objects to work with
        .map do |ivar|
          anno = ivar.annotation(IniOpt)
          {
            var_name:   ivar.name,
            ini_name:   anno[:ini_name] || ivar.name,
            transform:  anno[:transform] || ivar.type,
            deprecated: anno[:deprecated],
          }
        end
    %}

    # Generate a case branch for each INI setting in this section.
    # If a setting is marked as deprecated, look up the replacement instance variable
    # and redirect the value assignment to it instead, logging a deprecation warning.
    settings.each do |name, v|
      case name
        {% for var in ivars_in_section %}
         when "{{var[:ini_name]}}"
         {% if (deprecated = var[:deprecated]) %}
           Log.warn { "Config {{var[:ini_name]}} is deprecated, use {{deprecated.id}} instead" }
         {% end %}
         self.{{var[:var_name]}} = parse_value(v, {{var[:transform]}})
        {% end %}
     else
       Log.warn { "Unknown setting #{name} in section {{section.id}}" }
      end
    rescue ex
      Log.error { "Failed to handle value for '#{name}' in [{{section.id}}]: #{ex.message}" }
      abort
    end
  {% end %}
    end

    {% for int in [Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64] %}
      private def parse_value(value, type : {{int}}.class)
        {{int}}.new(value)
      end

      private def parse_value(value, type : {{int}}?.class)
        if v = value
          {{int}}.new(v)
        end
      end
    {% end %}

    private def parse_value(value, type : String.class | String?.class)
      value
    end

    private def parse_value(value, type : Bool.class)
      true?(value.downcase)
    end

    private def parse_value(value, type : Proc)
      type.call(value)
    end

    private def parse_value(value, type : Auth::Password::SHA256Password.class)
      Auth::Password::SHA256Password.new(value)
    end

    private def parse_bind(value)
      @amqp_bind = value
      @http_bind = value
    end

    def reload
      @sni_manager.clear
      parse_ini(@config_file)
      reload_logger
    end

    private def reload_logger
      log_file = (path = @log_file) ? File.open(path, "a") : STDOUT
      broadcast_backend = ::Log::BroadcastBackend.new
      backend = if journald_stream?
                  ::Log::IOBackend.new(io: log_file, formatter: JournalLogFormat)
                else
                  ::Log::IOBackend.new(io: log_file, formatter: StdoutLogFormat)
                end

      broadcast_backend.append(backend, @log_level)

      in_memory_backend = ::Log::InMemoryBackend.instance
      broadcast_backend.append(in_memory_backend, @log_level)

      ::Log.setup(@log_level, broadcast_backend)
      target = (path = @log_file) ? path : "stdout"
      Log.info &.emit("Logger settings", level: @log_level.to_s, target: target)
    end

    def journald_stream? : Bool
      return false unless journal_stream = ENV["JOURNAL_STREAM"]?
      return false if @log_file # If logging to a file, not using journald

      # JOURNAL_STREAM format is "device:inode"
      parts = journal_stream.split(':')
      return false unless parts.size == 2

      journal_dev = parts[0].to_u64?
      journal_ino = parts[1].to_u64?
      return false unless journal_dev && journal_ino

      # Get STDOUT's device and inode
      LibC.fstat(STDOUT.fd, out stat)
      stat.st_dev == journal_dev && stat.st_ino == journal_ino
    rescue
      false
    end

    def tls_configured?
      !@tls_cert_path.empty?
    end

    private def tcp_keepalive?(str : String?) : Tuple(Int32, Int32, Int32)?
      return if false?(str)
      if keepalive = str.try &.split(":")
        {
          keepalive[0]?.try(&.to_i?) || 60,
          keepalive[1]?.try(&.to_i?) || 10,
          keepalive[2]?.try(&.to_i?) || 3,
        }
      end
    end

    private def false?(str : String?)
      {"0", "false", "no", "off", "n"}.includes? str
    end

    private def true?(str : String?)
      {"1", "true", "yes", "on", "y"}.includes? str
    end

    # There is no guarantee that `@type.instance_vars` are sorted in the same way they are added in the code.
    # This struct is needed to simplify the sorting of the options array, because you cannot rely on the order of @type.instance_vars.
    struct Option
      include Comparable(Option)

      def self.new(short_flag : String, long_flag : String, description : String, deprecation_warn_msg : String?, &block : Proc(String, Nil))
        new(short_flag, long_flag, description, deprecation_warn_msg, block)
      end

      protected def initialize(@short_flag : String, @long_flag : String, @description : String, @deprecation_warn_msg : String?, @set_value : Proc(String, Nil))
      end

      def <=>(other : Option)
        self.compare_value <=> other.compare_value
      end

      # Sort options alphabetically by short flag. Options without short flags
      # are sorted to the end by prefixing their long flag with "z".
      protected def compare_value
        if @short_flag.empty?
          "z" + @long_flag
        else
          @short_flag
        end
      end

      def setup_parser(parser)
        if @short_flag.empty?
          do_setup_parser(parser, @long_flag, @description)
        else
          do_setup_parser(parser, @short_flag, @long_flag, @description)
        end
      end

      private def do_setup_parser(parser, *args)
        parser.on(*args) do |val|
          if msg = @deprecation_warn_msg
            Log.warn { msg }
          end
          @set_value.call(val)
        end
      end
    end
  end
end
