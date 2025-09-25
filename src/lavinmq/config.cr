require "log"
require "uri"
require "option_parser"
require "ini"
require "./version"
require "./log_formatter"
require "./in_memory_backend"
require "./auth/password"

module LavinMQ
  class Config
    annotation CliOpt; end
    annotation IniOpt; end
    annotation EnvOpt; end

    DEFAULT_LOG_LEVEL     = ::Log::Severity::Info
    DEFAULT_PASSWORD_HASH = "+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+" # Hash of 'guest'

    @[CliOpt("-c CONFIG", "--config=CONFIG", "Path to config file", section: "options")]
    @[EnvOpt("LAVINMQ_CONFIGURATION_DIRECTORY")]
    property config_file = ""

    @[CliOpt("-D DIRECTORY", "--data-dir=DIRECTORY", "Data directory", section: "options")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_DATADIR")]
    property data_dir : String = "/var/lib/lavinmq"

    @[CliOpt("-l LEVEL", "--log-level=LEVEL", "Log level (Default: info)", ->::Log::Severity.parse(String), section: "options")]
    @[IniOpt(section: "main", transform: ->::Log::Severity.parse(String))]
    property log_level : ::Log::Severity = DEFAULT_LOG_LEVEL

    @[CliOpt("-b BIND", "--bind=BIND", "IP address that both the AMQP and HTTP servers will listen on (default: 127.0.0.1)", ->parse_bind(String), section: "bindings")]
    property bind = "127.0.0.1"

    @[CliOpt("-p PORT", "--port=PORT", "AMQP port to listen on (default: 5672)", section: "bindings")]
    @[IniOpt(ini_name: port, section: "amqp")]
    @[EnvOpt("LAVINMQ_AMQP_PORT")]
    property amqp_port = 5672

    @[IniOpt(section: "main")]
    property log_file : String? = nil

    @[CliOpt("", "--amqp-bind=BIND", "IP address that the AMQP server will listen on (default: 127.0.0.1)", section: "bindings")]
    @[IniOpt(ini_name: bind, section: "amqp")]
    @[EnvOpt("LAVINMQ_AMQP_BIND")]
    property amqp_bind = "127.0.0.1"

    @[CliOpt("", "--amqp-unix-path=PATH", "AMQP UNIX path to listen to", section: "bindings")]
    @[IniOpt(ini_name: unix_path, section: "amqp")]
    property unix_path : String = ""

    @[CliOpt("", "--amqps-port=PORT", "AMQPS port to listen on (default: -1)", section: "bindings")]
    @[IniOpt(ini_name: tls_port, section: "amqp")]
    @[EnvOpt("LAVINMQ_AMQPS_PORT")]
    property amqps_port = -1

    @[IniOpt(ini_name: bind, section: "mqtt")]
    property mqtt_bind = "127.0.0.1"

    @[IniOpt(ini_name: port, section: "mqtt")]
    property mqtt_port = 1883

    @[IniOpt(ini_name: tls_port, section: "mqtt")]
    property mqtts_port = -1

    @[IniOpt(ini_name: unix_path, section: "mqtt")]
    property mqtt_unix_path = ""

    @[IniOpt(section: "amqp")]
    property unix_proxy_protocol = 1_u8 # PROXY protocol version on unix domain socket connections

    @[IniOpt(section: "amqp")]
    property tcp_proxy_protocol = 0_u8 # PROXY protocol version on amqp tcp connections

    @[CliOpt("", "--http-bind=BIND", "IP address that the HTTP server will listen on (default: 127.0.0.1)", section: "bindings")]
    @[IniOpt(ini_name: bind, section: "mgmt")]
    @[EnvOpt("LAVINMQ_HTTP_BIND")]
    property http_bind = "127.0.0.1"

    @[CliOpt("", "--http-port=PORT", "HTTP port to listen on (default: 15672)", section: "bindings")]
    @[IniOpt(ini_name: port, section: "mgmt")]
    @[EnvOpt("LAVINMQ_HTTP_PORT")]
    property http_port = 15672

    @[CliOpt("", "--http-unix-path=PATH", "HTTP UNIX path to listen to", section: "bindings")]
    @[IniOpt(ini_name: unix_path, section: "mgmt")]
    property http_unix_path = ""

    @[CliOpt("", "--https-port=PORT", "HTTPS port to listen on (default: -1)", section: "bindings")]
    @[IniOpt(ini_name: tls_port, section: "mgmt")]
    @[EnvOpt("LAVINMQ_HTTPS_PORT")]
    property https_port = -1

    @[CliOpt("", "--cert FILE", "TLS certificate (including chain)", section: "tls")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_TLS_CERT_PATH")]
    property tls_cert_path = ""

    @[CliOpt("", "--ciphers CIPHERS", "List of TLS ciphers to allow", section: "tls")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_TLS_CIPHERS")]
    property tls_ciphers = ""

    @[CliOpt("", "--key FILE", "Private key for the TLS certificate", section: "tls")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_TLS_KEY_PATH")]
    property tls_key_path = ""

    @[CliOpt("", "--tls-min-version=VERSION", "Mininum allowed TLS version (default 1.2)", section: "tls")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_TLS_MIN_VERSION")]
    property tls_min_version = ""

    @[IniOpt(section: "mgmt")]
    property http_systemd_socket_name = "lavinmq-http.socket"

    @[IniOpt(section: "amqp")]
    property amqp_systemd_socket_name = "lavinmq-amqp.socket"

    @[IniOpt(section: "amqp")]
    property heartbeat = 300_u16 # second

    @[IniOpt(section: "amqp")]
    property frame_max = 131_072_u32 # bytes

    @[IniOpt(section: "amqp")]
    property channel_max = 2048_u16 # number

    @[IniOpt(section: "main")]
    property stats_interval = 5000 # millisecond

    @[IniOpt(section: "main")]
    property stats_log_size = 120 # 10 mins at 5s interval

    @[IniOpt(section: "main")]
    property? set_timestamp = false # in message headers when receive

    @[IniOpt(section: "main")]
    property socket_buffer_size = 16384 # bytes

    @[IniOpt(section: "main")]
    property? tcp_nodelay = false # bool

    @[IniOpt(section: "main")]
    property segment_size : Int32 = 8 * 1024**2 # bytes

    @[IniOpt(section: "mqtt")]
    property max_inflight_messages : UInt16 = 65_535 # mqtt messages

    @[IniOpt(section: "mqtt")]
    property default_mqtt_vhost = "/"

    @[IniOpt(section: "main", transform: ->tcp_keepalive?(String))]
    property tcp_keepalive : Tuple(Int32, Int32, Int32)? = {60, 10, 3} # idle, interval, probes/count

    @[IniOpt(section: "main")]
    property tcp_recv_buffer_size : Int32? = nil

    @[IniOpt(section: "main")]
    property tcp_send_buffer_size : Int32? = nil

    @[IniOpt(section: "amqp")]
    property max_message_size = 128 * 1024**2

    @[IniOpt(section: "main")]
    property? log_exchange : Bool = false

    @[IniOpt(section: "main")]
    property free_disk_min : Int64 = 0 # bytes

    @[IniOpt(section: "main")]
    property free_disk_warn : Int64 = 0 # bytes

    @[IniOpt(section: "main")]
    property max_deleted_definitions = 8192 # number of deleted queues, unbinds etc that compacts the definitions file

    @[IniOpt(section: "main")]
    property consumer_timeout : UInt64? = nil

    @[IniOpt(section: "main")]
    property consumer_timeout_loop_interval = 60 # seconds

    @[IniOpt(section: "experimental")]
    property yield_each_received_bytes = 131_072 # max number of bytes to read from a client connection without letting other tasks in the server do any work

    @[IniOpt(section: "experimental")]
    property yield_each_delivered_bytes = 1_048_576 # max number of bytes sent to a client without tending to other tasks in the server

    @[IniOpt(section: "main", transform: ->(s : String) { s.split(",").map(&.strip) })]
    property auth_backends : Array(String) = ["basic"]

    @[CliOpt("", "--default-consumer-prefetch=NUMBER", "Default consumer prefetch (default 65535)", section: "options")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_DEFAULT_CONSUMER_PREFETCH")]
    property default_consumer_prefetch = UInt16::MAX

    @[CliOpt("", "--default-password=PASSWORD-HASH", "(Deprecated) Hashed password for default user (default: '+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+' (guest))", deprecated: "default_password_hash", section: "options")]
    @[IniOpt(section: "main", deprecated: "default_password_hash")]
    property default_password = DEFAULT_PASSWORD_HASH # Hashed password for default user

    @[CliOpt("", "--default-password-hash=PASSWORD-HASH", "Hashed password for default user (default: '+pHuxkR9fCyrrwXjOD4BP4XbzO3l8LJr8YkThMgJ0yVHFRE+' (guest))", section: "options")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_DEFAULT_PASSWORD")]
    property default_password_hash = DEFAULT_PASSWORD_HASH # Hashed password for default user

    @[CliOpt("", "--default-user=USER", "Default user (default: guest)", section: "options")]
    @[IniOpt(section: "main")]
    @[EnvOpt("LAVINMQ_DEFAULT_USER")]
    property default_user = "guest"

    @[CliOpt("", "--default-user-only-loopback=BOOL", "Limit guest user to only connect from loopback address", section: "options")]
    @[IniOpt(section: "amqp")]
    property? default_user_only_loopback : Bool = true

    @[CliOpt("", "--guest-only-loopback=BOOL", "Limit guest user to only connect from loopback address", deprecated: "Use --default-user-only-loopback instead.", section: "options")]
    @[IniOpt(section: "main", deprecated: "default_user_only_loopback")]
    property? guest_only_loopback : Bool = true

    @[CliOpt("", "--no-data-dir-lock", "Don't put a file lock in the data directory (default true)", section: "options")]
    @[IniOpt(section: "main")]
    property? data_dir_lock : Bool = true

    @[CliOpt("", "--raise-gc-warn", "Raise on GC warnings", section: "options")]
    property? raise_gc_warn : Bool = false

    @[CliOpt("", "--clustering", "Enable clustering", section: "clustering")]
    @[IniOpt(ini_name: enabled, section: "clustering")]
    @[EnvOpt("LAVINMQ_CLUSTERING")]
    property? clustering = false

    @[CliOpt("", "--clustering-advertised-uri=URI", "Advertised URI for the clustering server", section: "clustering")]
    @[IniOpt(ini_section: clustering)]
    @[EnvOpt("LAVINMQ_CLUSTERING_ADVERTISED_URI")]
    property clustering_advertised_uri : String? = nil

    @[CliOpt("", "--clustering-bind=BIND", "Listen for clustering followers on this address (default: localhost)", section: "clustering")]
    @[IniOpt(ini_name: bind, section: "clustering")]
    @[EnvOpt("LAVINMQ_CLUSTERING_BIND")]
    property clustering_bind = "127.0.0.1"

    @[CliOpt("", "--clustering-etcd-endpoints=URIs", "Comma separeted host/port pairs (default: 127.0.0.1:2379)", section: "clustering")]
    @[IniOpt(ini_name: etcd_endpoints, section: "clustering")]
    @[EnvOpt("LAVINMQ_CLUSTERING_ETCD_ENDPOINTS")]
    property clustering_etcd_endpoints = "localhost:2379"

    @[CliOpt("", "--clustering-etcd-prefix=KEY", "Key prefix used in etcd (default: lavinmq", section: "clustering")]
    @[IniOpt(ini_name: etcd_prefix, section: "clustering")]
    @[EnvOpt("LAVINMQ_CLUSTERING_ETCD_PREFIX")]
    property clustering_etcd_prefix = "lavinmq"

    @[CliOpt("", "--clustering-max-unsynced-actions=ACTIONS", "Maximum unsynced actions", section: "clustering")]
    @[IniOpt(ini_name: max_unsynced_actions, section: "clustering")]
    @[EnvOpt("LAVINMQ_CLUSTERING_MAX_UNSYNCED_ACTIONS")]
    property clustering_max_unsynced_actions = 8192 # number of unsynced clustering actions

    @[CliOpt("", "--clustering-port=PORT", "Listen for clustering followers on this port (default: 5679)", section: "clustering")]
    @[IniOpt(ini_name: port, section: "clustering")]
    @[EnvOpt("LAVINMQ_CLUSTERING_PORT")]
    property clustering_port = 5679

    property max_consumers_per_channel = 0
    @@instance : Config = self.new

    def self.instance : LavinMQ::Config
      @@instance
    end

    private def initialize
    end

    # Parse configuration from environment, command line arguments and configuration file.
    # Command line arguments take precedence over environment variables,
    # which take precedence over the configuration file.
    def parse
      @config_file = File.exists?(
        File.join(ENV.fetch("LAVINMQ_CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini")) ? File.join(ENV.fetch("LAVINMQ_CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini") : ""
      parse_argv(warn_deprecated: true) # get config_file
      parse_ini(@config_file)
      parse_env()
      parse_argv()
    end

    private def parse_env
      {% for ivar in @type.instance_vars.select(&.annotation(EnvOpt)) %}
        {% env_name, transform = ivar.annotation(EnvOpt).args %}
        if v = ENV.fetch({{env_name}}, nil)
          @{{ivar}} = parse_value(v, {{transform || ivar.type}})
        end
      {% end %}
    end

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

      protected def compare_value
        if @short_flag.empty?
          "z" + @long_flag
        else
          @short_flag
        end
      end

      def setup_parser(parser, warn_deprecated)
        if @short_flag.empty?
          do_setup_parser(parser, warn_deprecated, @long_flag, @description)
        else
          do_setup_parser(parser, warn_deprecated, @short_flag, @long_flag, @description)
        end
      end

      private def do_setup_parser(parser, warn_deprecated, *args)
        parser.on(*args) do |val|
          if warn_deprecated && (msg = @deprecation_warn_msg)
            Log.warn { msg }
          end
          @set_value.call(val)
        end
      end
    end

    private def parse_argv(*, warn_deprecated = false)
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
            if cli_opt.args.size == 3
              parser_arg = cli_opt.args
              value_parser = ivar.type
            else
              *parser_arg, value_parser = cli_opt.args
            end
            section_id = cli_opt[:section] || "options"
            # sections[section.id][:options] << {ivar: ivar.name.id, parser_arg: parser_arg, value_parser: value_parser, deprecated: cli_opt[:deprecated]}
          %}
          sections[:{{section_id}}][:options] << Option.new({{parser_arg.splat}}, {{cli_opt[:deprecated]}}) do |value|
            @{{ivar.name.id}} = parse_value(value, {{value_parser}})
          end
        {% end %}
        sections.each do |_section_id, section|
          parser.separator "\n#{section[:description]}"
          section[:options].sort.each do |opt|
            opt.setup_parser(parser, warn_deprecated)
          end
        end
      {% end %}
      parser.separator "\nMiscellaneous"
      parser.on("-h", "--help", "Show this help") { puts parser; exit 0 }
      parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
      parser.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
      parser.parse(ARGV.dup)
    end

    # Generate parse_value methods for all Int and UInt
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
      %w[on yes true 1].includes?(value.downcase)
    end

    private def parse_value(value, type : Proc)
      type.call(value)
    end

    private def parse_bind(value)
      @amqp_bind = value
      @http_bind = value
    end

    private def parse_ini(file)
      return if file.empty?
      abort "Config could not be found" unless File.file?(file)
      ini = INI.parse(File.read(file))
      ini.each do |section, settings|
        case section
        when "main"
          parse_section("main", settings)
        when "amqp"
          parse_section("amqp", settings)
        when "mqtt"
          parse_section("mqtt", settings)
        when "mgmt"
          parse_section("mgmt", settings)
        when "clustering"
          parse_section("clustering", settings)
        when "replication"
          abort("#{file}: [replication] is deprecated and replaced with [clustering], see the README for more information")
        when "experimental"
          parse_section("experimental", settings)
        else
          raise "Unknown configuration section: #{section}"
        end
      end
    rescue ex : ::INI::ParseException
      abort "Failed to parse config file '#{file}'. " \
            "Error on line #{ex.line_number}, column #{ex.column_number}"
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

    settings.each do |name, v|
      case name
        {% for var in ivars_in_section %}
         when "{{var[:ini_name]}}"
         {% if (deprecated = var[:deprecated]) %}
           {%
             use_ivar = @type.instance_vars.find &.name.== deprecated
             anno = use_ivar.annotation(IniOpt)
             use_ivar = {
               var_name:   use_ivar.name,
               ini_name:   anno[:ini_name] || use_ivar.name,
               transform:  anno[:transform] || use_ivar.type,
               deprecated: anno[:deprecated],
             }
           %}
           Log.warn { "Config {{var[:ini_name]}} is depricated, use {{use_ivar[:ini_name]}} instead" }
           {% var = use_ivar %}
         {% end %}
         @{{var[:var_name]}} = parse_value(v, {{var[:transform]}})
        {% end %}
     else
       raise "Unknown setting in section (ivars_in_section: {{ivars_in_section.map(&.[](:var_name)).join(", ").id}})"
      end
    rescue ex
      Log.error { "Failed to handle value for '#{name}' in [{{section.id}}]: #{ex.message}" }
      abort
    end
  {% end %}
    end

    def reload
      parse_ini(@config_file)
      reload_logger
    end

    private def reload_logger
      log_file = (path = @log_file) ? File.open(path, "a") : STDOUT
      broadcast_backend = ::Log::BroadcastBackend.new
      backend = if ENV.has_key?("JOURNAL_STREAM")
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

    def tls_configured?
      !@tls_cert_path.empty?
    end

    private def tcp_keepalive?(str : String?) : Tuple(Int32, Int32, Int32)?
      return nil if false?(str)
      if keepalive = str.try &.split(":")
        {
          keepalive[0]?.try(&.to_i?) || 60,
          keepalive[1]?.try(&.to_i?) || 10,
          keepalive[2]?.try(&.to_i?) || 3,
        }
      end
    end

    private def false?(str : String?)
      {"0", "false", "no", "off"}.includes? str
    end
  end
end
