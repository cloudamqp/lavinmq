require "option_parser"
require "ini"
require "./config"

module AvalancheMQ
  class ServerCLI
    getter parser

    def initialize(@config : Config, @config_file : String)
      @parser = OptionParser.parse do |parser|
        parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
        parser.on("-b BIND", "--bind=BIND", "IP address that both the AMQP and HTTP servers will listen on (default: 127.0.0.1)") do |v|
          config.amqp_bind = v
          config.http_bind = v
        end
        parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| @config_file = v }
        parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| config.data_dir = v }
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
          level = Logger::Severity.parse?(v.to_s)
          config.log_level = level if level
        end
        parser.on("-d", "--debug", "Verbose logging") { config.log_level = Logger::DEBUG }
        parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
        parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
        parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
      end
    end

    def parse
      @parser.parse
      unless @config_file.empty?
        @config.config_file = @config_file # Remember file for reloads
        @config.parse(@config_file)
      end
      if @config.data_dir.empty?
        STDERR.puts "No data directory specified"
        STDERR.puts @parser
        exit 2
      end
    end
  end
end
