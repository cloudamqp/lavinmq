require "option_parser"
require "uri"

module LavinMQPerf
  class Perf
    @uri = URI.parse "amqp://guest:guest@localhost"
    @io : IO
    @err_io : IO
    getter mqtt_banner
    getter amqp_banner

    def initialize(@io : IO = STDOUT, @err_io : IO = STDERR)
      @parser = OptionParser.new
      @amqp_banner = "Usage: #{PROGRAM_NAME} [protocol] [throughput | bind-churn | queue-churn | connection-churn | connection-count | queue-count] [arguments]"
      @mqtt_banner = "Usage: #{PROGRAM_NAME} [protocol] [throughput]"
      @parser.banner = @amqp_banner
      @parser.on("-h", "--help", "Show this help") { @io.puts @parser; exit 0 }
      @parser.on("-v", "--version", "Show version") { @io.puts LavinMQ::VERSION; exit 0 }
      @parser.on("--build-info", "Show build information") { @io.puts BUILD_INFO; exit 0 }
      @parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
      @parser.unknown_args { |args| abort "Unexpected argument: #{args.join(", ")}\n\n#{@parser}" unless args.empty? }
      @parser.on("--uri=URI", "URI to connect to (default #{@uri})") do |v|
        @uri = URI.parse(v)
      end
    end

    def run(args = ARGV)
      @parser.parse(args)
    end

    # Override the top-level `abort` so error output goes through `@err_io`
    # (STDERR by default, a captured IO in specs) and can be redirected.
    private def abort(message = nil, status = 1) : NoReturn
      @err_io.puts message if message
      exit status
    end

    # Parses an integer option value, aborting with a message instead of
    # raising when it isn't a number or is outside min..max.
    private def parse_int(value : String, name : String, min = 0, max = Int32::MAX) : Int32
      i = value.to_i64? || abort "#{name} must be a number, got #{value}"
      if max == Int32::MAX
        abort "#{name} must be between #{min} and #{max}, got #{i}" if i > max
        abort "#{name} must be >= #{min}, got #{i}" if i < min
      else
        abort "#{name} must be between #{min} and #{max}, got #{i}" unless min <= i <= max
      end
      i.to_i32
    end

    macro build_flags
      {%
        flags = [] of String
        flags << "--release" if flag?(:release)
        flags << "--debug" if flag?(:debug)
        flags << "--no-debug" unless flag?(:debug)
        flags << "--Dmt" if flag?(:mt)
      %}
      {{ flags.join(" ") }}
    end

    def rss
      File.read("/proc/self/statm").split[1].to_i64 * 4096
    rescue File::NotFoundError
      if ps_rss = `ps -o rss= -p $PPID`.to_i64?
        ps_rss * 1024
      else
        0
      end
    end

    BUILD_INFO = <<-INFO
        LavinMQPerf #{LavinMQ::VERSION}
        #{Crystal::DESCRIPTION.lines.reject(&.empty?).join("\n")}
        Build flags: #{build_flags}
      INFO
  end
end
