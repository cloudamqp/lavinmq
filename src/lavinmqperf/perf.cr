require "option_parser"
require "uri"

module LavinMQPerf
  class Perf
    @uri = URI.parse "amqp://guest:guest@localhost"
    getter banner

    def initialize
      @parser = OptionParser.new
      @banner = "Usage: #{PROGRAM_NAME} [throughput | bind-churn | queue-churn | connection-churn | connection-count | queue-count] [arguments]"
      @parser.banner = @banner
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

    BUILD_INFO = <<-INFO
    LavinMQPerf #{LavinMQ::VERSION}
    #{Crystal::DESCRIPTION.lines.reject(&.empty?).join("\n")}
    Build flags: #{build_flags}
    INFO
  end
end
