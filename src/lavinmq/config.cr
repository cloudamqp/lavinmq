require "log"
require "uri"
require "option_parser"
require "ini"
require "./version"

module LavinMQ
  class Config
    DEFAULT_LOG_LEVEL = Log::Severity::Info

    property data_dir : String = ENV.fetch("STATE_DIRECTORY", "/var/lib/lavinmq")
    property config_file = File.exists?(File.join(ENV.fetch("CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini")) ? File.join(ENV.fetch("CONFIGURATION_DIRECTORY", "/etc/lavinmq"), "lavinmq.ini") : ""
    property log_file : String? = nil
    property log_level : Log::Severity = DEFAULT_LOG_LEVEL
    property amqp_bind = "127.0.0.1"
    property amqp_port = 5672
    property amqps_port = -1
    property unix_path = ""
    property unix_proxy_protocol = 1_u8 # PROXY protocol version on unix domain socket connections
    property tcp_proxy_protocol = 0_u8  # PROXY protocol version on amqp tcp connections
    property tls_cert_path = ""
    property tls_key_path = ""
    property tls_ciphers = ""
    property tls_min_version = ""
    property http_bind = "127.0.0.1"
    property http_port = 15672
    property https_port = -1
    property http_unix_path = ""
    property http_systemd_socket_name = "lavinmq-http.socket"
    property amqp_systemd_socket_name = "lavinmq-amqp.socket"
    property heartbeat = 300_u16                # second
    property frame_max = 131_072_u32            # bytes
    property channel_max = 2048_u16             # number
    property stats_interval = 5000              # millisecond
    property stats_log_size = 120               # 10 mins at 5s interval
    property? set_timestamp = false             # in message headers when receive
    property socket_buffer_size = 16384         # bytes
    property? tcp_nodelay = false               # bool
    property segment_size : Int32 = 8 * 1024**2 # bytes
    property? raise_gc_warn : Bool = false
    property? data_dir_lock : Bool = true
    property tcp_keepalive : Tuple(Int32, Int32, Int32)? = {60, 10, 3} # idle, interval, probes/count
    property tcp_recv_buffer_size : Int32? = nil
    property tcp_send_buffer_size : Int32? = nil
    property? guest_only_loopback : Bool = true
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
    property clustering_max_lag = 8192      # number of clustering actions
    property clustering_min_isr = 0         # number of In Sync Replicas required at all time
    property max_deleted_definitions = 8192 # number of deleted queues, unbinds etc that compacts the definitions file
    property consumer_timeout : UInt64? = nil
    property consumer_timeout_loop_interval = 60 # seconds
    @@instance : Config = self.new

    def self.instance : LavinMQ::Config
      @@instance
    end

    private def initialize
      @parser = OptionParser.new do |parser|
        parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
        parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| @config_file = v }
        parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| @data_dir = v }
        parser.on("-b BIND", "--bind=BIND", "IP address that both the AMQP and HTTP servers will listen on (default: 127.0.0.1)") do |v|
          @amqp_bind = v
          @http_bind = v
        end
        parser.on("-p PORT", "--amqp-port=PORT", "AMQP port to listen on (default: 5672)") do |v|
          @amqp_port = v.to_i
        end
        parser.on("--amqps-port=PORT", "AMQPS port to listen on (default: -1)") do |v|
          @amqps_port = v.to_i
        end
        parser.on("--amqp-bind=BIND", "IP address that the AMQP server will listen on (default: 127.0.0.1)") do |v|
          @amqp_bind = v
        end
        parser.on("--http-port=PORT", "HTTP port to listen on (default: 15672)") do |v|
          @http_port = v.to_i
        end
        parser.on("--https-port=PORT", "HTTPS port to listen on (default: -1)") do |v|
          @https_port = v.to_i
        end
        parser.on("--http-bind=BIND", "IP address that the HTTP server will listen on (default: 127.0.0.1)") do |v|
          @http_bind = v
        end
        parser.on("--amqp-unix-path=PATH", "AMQP UNIX path to listen to") do |v|
          @unix_path = v
        end
        parser.on("--http-unix-path=PATH", "HTTP UNIX path to listen to") do |v|
          @http_unix_path = v
        end
        parser.on("--cert FILE", "TLS certificate (including chain)") { |v| @tls_cert_path = v }
        parser.on("--key FILE", "Private key for the TLS certificate") { |v| @tls_key_path = v }
        parser.on("--ciphers CIPHERS", "List of TLS ciphers to allow") { |v| @tls_ciphers = v }
        parser.on("--tls-min-version=VERSION", "Mininum allowed TLS version (default 1.2)") { |v| @tls_min_version = v }
        parser.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |v|
          level = Log::Severity.parse?(v.to_s)
          @log_level = level if level
        end
        parser.on("--raise-gc-warn", "Raise on GC warnings") { @raise_gc_warn = true }
        parser.on("--no-data-dir-lock", "Don't put a file lock in the data directory (default true)") { @data_dir_lock = false }
        parser.on("-d", "--debug", "Verbose logging") { @log_level = Log::Severity::Debug }
        parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
        parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
        parser.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
        parser.on("--guest-only-loopback=BOOL", "Limit guest user to only connect from loopback address") do |v|
          @guest_only_loopback = {"true", "yes", "y", "1"}.includes? v.to_s
        end
        parser.on("--clustering", "Enable clustering") do
          @clustering = true
        end
        parser.on("--clustering-advertised-uri=URI", "Advertised URI for the clustering server") do |v|
          @clustering_advertised_uri = v
        end
        parser.on("--clustering-etcd-prefix=KEY", "Key prefix used in etcd (default: lavinmq)") do |v|
          @clustering_etcd_prefix = v
        end
        parser.on("--clustering-port=PORT", "Listen for clustering followers on this port (default: 5679)") do |v|
          @clustering_port = v.to_i
        end
        parser.on("--clustering-bind=BIND", "Listen for clustering followers on this address (default: localhost)") do |v|
          @clustering_bind = v
        end
        parser.on("--clustering-max-lag=ACTIONS", "Max unsynced replicated messages") do |v|
          @clustering_max_lag = v.to_i
        end
        parser.on("--clustering-min-isr=COUNT", "Required in-sync-replicas") do |v|
          @clustering_min_isr = v.to_i
        end
        parser.on("--clustering-etcd-endpoints=URIs", "Comma separeted host/port pairs (default: 127.0.0.1:2379)") do |v|
          @clustering_etcd_endpoints = v
        end
        parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
      end
    end

    def parse
      @parser.parse(ARGV.dup) # only parse args to get config_file
      parse(@config_file)
      @parser.parse(ARGV.dup) # then override any config_file parameters with the cmd line args
      if @data_dir.empty?
        STDERR.puts "No data directory specified"
        STDERR.puts @parser
        exit 2
      end
    rescue ex
      abort ex.message
    end

    def parse(file)
      return if file.empty?
      abort "Config could not be found" unless File.file?(file)
      ini = INI.parse(File.read(file))
      ini.each do |section, settings|
        case section
        when "main"         then parse_main(settings)
        when "amqp"         then parse_amqp(settings)
        when "mgmt", "http" then parse_mgmt(settings)
        when "clustering"   then parse_clustering(settings)
        else
          raise "Unrecognized config section: #{section}"
        end
      end
    end

    def tls_configured?
      !@tls_cert_path.empty?
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_main(settings)
      settings.each do |config, v|
        case config
        when "data_dir"                then @data_dir = v
        when "data_dir_lock"           then @data_dir_lock = true?(v)
        when "log_level"               then @log_level = Log::Severity.parse(v)
        when "log_file"                then @log_file = v
        when "stats_interval"          then @stats_interval = v.to_i32
        when "stats_log_size"          then @stats_log_size = v.to_i32
        when "segment_size"            then @segment_size = v.to_i32
        when "set_timestamp"           then @set_timestamp = true?(v)
        when "socket_buffer_size"      then @socket_buffer_size = v.to_i32
        when "tcp_nodelay"             then @tcp_nodelay = true?(v)
        when "tcp_keepalive"           then @tcp_keepalive = tcp_keepalive?(v)
        when "tcp_recv_buffer_size"    then @tcp_recv_buffer_size = v.to_i32?
        when "tcp_send_buffer_size"    then @tcp_send_buffer_size = v.to_i32?
        when "tls_cert"                then @tls_cert_path = v
        when "tls_key"                 then @tls_key_path = v
        when "tls_ciphers"             then @tls_ciphers = v
        when "tls_min_version"         then @tls_min_version = v
        when "guest_only_loopback"     then @guest_only_loopback = true?(v)
        when "log_exchange"            then @log_exchange = true?(v)
        when "free_disk_min"           then @free_disk_min = v.to_i64
        when "free_disk_warn"          then @free_disk_warn = v.to_i64
        when "max_deleted_definitions" then @max_deleted_definitions = v.to_i
        when "consumer_timeout"        then @consumer_timeout = v.to_u64
        else
          STDERR.puts "WARNING: Unrecognized configuration 'main/#{config}'"
        end
      end
    end

    private def parse_clustering(settings)
      settings.each do |config, v|
        case config
        when "enabled"        then @clustering = true?(v)
        when "etcd_prefix"    then @clustering_etcd_prefix = v
        when "etcd_endpoints" then @clustering_etcd_endpoints = v
        when "advertised_uri" then @clustering_advertised_uri = v
        when "bind"           then @clustering_bind = v
        when "port"           then @clustering_port = v.to_i32
        when "max_lag"        then @clustering_max_lag = v.to_i32
        when "min_isr"        then @clustering_min_isr = v.to_i32
        else
          STDERR.puts "WARNING: Unrecognized configuration 'clustering/#{config}'"
        end
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    private def parse_amqp(settings)
      settings.each do |config, v|
        case config
        when "bind"                then @amqp_bind = v
        when "port"                then @amqp_port = v.to_i32
        when "tls_port"            then @amqps_port = v.to_i32
        when "tls_cert"            then @tls_cert_path = v # backward compatibility
        when "tls_key"             then @tls_key_path = v  # backward compatibility
        when "unix_path"           then @unix_path = v
        when "heartbeat"           then @heartbeat = v.to_u16
        when "frame_max"           then @frame_max = v.to_u32
        when "channel_max"         then @channel_max = v.to_u16
        when "max_message_size"    then @max_message_size = v.to_i32
        when "systemd_socket_name" then @amqp_systemd_socket_name = v
        when "unix_proxy_protocol" then @unix_proxy_protocol = true?(v) ? 1u8 : v.to_u8? || 0u8
        when "tcp_proxy_protocol"  then @tcp_proxy_protocol = true?(v) ? 1u8 : v.to_u8? || 0u8
        else
          STDERR.puts "WARNING: Unrecognized configuration 'amqp/#{config}'"
        end
      end
    end

    private def parse_mgmt(settings)
      settings.each do |config, v|
        case config
        when "bind"                then @http_bind = v
        when "port"                then @http_port = v.to_i32
        when "tls_port"            then @https_port = v.to_i32
        when "tls_cert"            then @tls_cert_path = v # backward compatibility
        when "tls_key"             then @tls_key_path = v  # backward compatibility
        when "unix_path"           then @http_unix_path = v
        when "systemd_socket_name" then @http_systemd_socket_name = v
        else
          STDERR.puts "WARNING: Unrecognized configuration 'mgmt/#{config}'"
        end
      end
    end

    private def true?(str : String?)
      {"true", "yes", "y", "1"}.includes? str
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
