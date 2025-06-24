require "option_parser"
require "uri"

module LavinMQPerf
  class Perf
    @uri = URI.parse "amqp://guest:guest@localhost"
    getter mqtt_banner
    getter amqp_banner

    def initialize
      @parser = OptionParser.new
      @amqp_banner = "Usage: #{PROGRAM_NAME} [protocol] [throughput | bind-churn | queue-churn | connection-churn | connection-count | queue-count] [arguments]"
      @mqtt_banner = "Usage: #{PROGRAM_NAME} [protocol] [throughput]"
      @parser.banner = @amqp_banner
      @parser.on("-h", "--help", "Show this help") { puts @parser; exit 0 }
      @parser.on("-v", "--version", "Show version") { puts LavinMQ::VERSION; exit 0 }
      @parser.on("--build-info", "Show build information") { puts BUILD_INFO; exit 0 }
      @parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
      @parser.on("--uri=URI", "URI to connect to (default #{@uri})") do |v|
        @uri = URI.parse(v)
      end
    end

    def run(args = ARGV)
      @parser.parse(args)
    end

    macro build_flags
      {%
        flags = [] of String
        flags << "--release" if flag?(:release)
        flags << "--debug" if flag?(:debug)
        flags << "--no-debug" unless flag?(:debug)
        flags << "--Dpreview_mt" if flag?(:preview_mt)
        flags << "--Dmt" if flag?(:mt)
      %}
      {{flags.join(" ")}}
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
