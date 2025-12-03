require "log"
require "uri"
require "option_parser"
require "ini"
require "./version"
require "./log_formatter"
require "./in_memory_backend"
require "./auth/password"
require "./config/options"

module LavinMQ
  class Config
    include Options
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
      parse_config_from_cli
      parse_ini(@config_file)
      parse_env()
      parse_cli()
    end

    private def parse_config_from_cli
      parser = OptionParser.new
      parser.on("-c CONFIG", "--config=CONFIG", "Path to config file") do |val|
        @config_file = val
      end
      parser.invalid_option { }
      parser.missing_option { }
      parser.parse(ARGV.dup)
    end

    private def parse_env
      {% for ivar in @type.instance_vars.select(&.annotation(EnvOpt)) %}
        {% env_name, transform = ivar.annotation(EnvOpt).args %}
        if v = ENV.fetch({{env_name}}, nil)
          @{{ivar}} = parse_value(v, {{transform || ivar.type}})
        end
      {% end %}
    end

    private def parse_cli
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
            opt.setup_parser(parser)
          end
        end
      {% end %}
      parser.separator "\nMiscellaneous"
      parser.on("-h", "--help", "Show this help") { puts parser; exit 0 }
      parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
      parser.on("--build-info", "Show build information") { puts LavinMQ::BUILD_INFO; exit 0 }
      parser.parse(ARGV.dup)
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
       raise "Unknown setting #{name} in section {{section.id}}"
      end
    rescue ex
      Log.error { "Failed to handle value for '#{name}' in [{{section.id}}]: #{ex.message}" }
      abort
    end
  {% end %}
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
      {"0", "false", "no", "off", "n"}.includes? str
    end

    private def true?(str : String?)
      {"1", "true", "yes", "on", "y"}.includes? str
    end

    # There is no guarantee that `@type.instance_vars` are sorted in the same way they are added in the code.
    # This struct is needed simplify the sorting of the options array, becasue you cannot rely on the order of @type.instance_vars.
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
