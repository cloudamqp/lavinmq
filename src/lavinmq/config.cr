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
require "./ip_matcher"

module LavinMQ
  class Config
    include Options
    @@instance : Config = self.new
    getter sni_manager : SNIManager = SNIManager.new

    def self.instance : LavinMQ::Config
      @@instance
    end

    private def initialize
    end

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
          raise "Unknown configuration section: #{section}"
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
