require "option_parser"
require "ini"
require "./config"

module LavinMQ
  class ServerCLI
    getter parser

    def initialize(@config : Config)
      @parser = OptionParser.new do |parser|
        parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
        parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| config.config_file = v }
        parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| config.data_dir = v }
        parser.on("-b BIND", "--bind=BIND", "IP address that both the AMQP and HTTP servers will listen on (default: 127.0.0.1)") do |v|
          config.amqp_bind = v
          config.http_bind = v
        end
        parser.on("-p PORT", "--amqp-port=PORT", "AMQP port to listen on (default: 5672)") do |v|
          config.amqp_port = v.to_i
        end
        parser.on("--amqps-port=PORT", "AMQPS port to listen on (default: -1)") do |v|
          config.amqps_port = v.to_i
        end
        parser.on("--amqp-bind=BIND", "IP address that the AMQP server will listen on (default: 127.0.0.1)") do |v|
          config.amqp_bind = v
        end
        parser.on("--http-port=PORT", "HTTP port to listen on (default: 15672)") do |v|
          config.http_port = v.to_i
        end
        parser.on("--https-port=PORT", "HTTPS port to listen on (default: -1)") do |v|
          config.https_port = v.to_i
        end
        parser.on("--http-bind=BIND", "IP address that the HTTP server will listen on (default: 127.0.0.1)") do |v|
          config.http_bind = v
        end
        parser.on("--amqp-unix-path=PATH", "AMQP UNIX path to listen to") do |v|
          config.unix_path = v
        end
        parser.on("--http-unix-path=PATH", "HTTP UNIX path to listen to") do |v|
          config.http_unix_path = v
        end
        parser.on("--cert FILE", "TLS certificate (including chain)") { |v| config.tls_cert_path = v }
        parser.on("--key FILE", "Private key for the TLS certificate") { |v| config.tls_key_path = v }
        parser.on("--ciphers CIPHERS", "List of TLS ciphers to allow") { |v| config.tls_ciphers = v }
        parser.on("--tls-min-version=VERSION", "Mininum allowed TLS version (default 1.2)") { |v| config.tls_min_version = v }
        parser.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |v|
          level = ::Log::Severity.parse?(v.to_s)
          config.log_level = level if level
        end
        parser.on("--raise-gc-warn", "Raise on GC warnings") { config.raise_gc_warn = true }
        parser.on("--no-data-dir-lock", "Don't put a file lock in the data directory (default true)") { config.data_dir_lock = false }
        parser.on("-d", "--debug", "Verbose logging") { config.log_level = ::Log::Severity::Debug }
        parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
        parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
        parser.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
        parser.on("--guest-only-loopback=BOOL", "Limit guest user to only connect from loopback address") do |v|
          config.guest_only_loopback = {"true", "yes", "y", "1"}.includes? v.to_s
        end
        parser.on("-F LEADER-URI", "--follow=LEADER-URI", "Follow/replicate a leader node") do |v|
          config.replication_follow = URI.parse(v)
        end
        parser.on("--replication-port=PORT", "Listen for replication followers on this port (default: 5679)") do |v|
          config.replication_port = v.to_i
        end
        parser.on("--replication-bind=BIND", "Listen for replication followers on this address (default: none)") do |v|
          config.replication_bind = v
        end
        parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
      end
    end

    def parse
      @parser.parse(ARGV.dup) # only parse args to get config_file
      @config.parse(@config.config_file) unless @config.config_file.empty?
      @parser.parse(ARGV.dup) # then override any config_file parameters with the cmd line args
      if @config.data_dir.empty?
        STDERR.puts "No data directory specified"
        STDERR.puts @parser
        exit 2
      end
    rescue ex
      abort ex.message
    end
  end
end
