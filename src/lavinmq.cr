require "./lavinmq/version"
require "./stdlib/*"
require "./lavinmq/config"

config = LavinMQ::Config.instance
begin
  config.parse # both ARGV and config file
rescue ex : OptionParser::Exception
  abort "Error: #{ex.message}\nTry '#{PROGRAM_NAME} --help' for more information."
rescue ex : LavinMQ::Config::Error
  abort "Error: #{ex.message}"
end

{% unless flag?(:gc_none) %}
  case grow_mark_stack = ENV["LAVINMQ_EXPERIMENTAL_GC_GROW_MARK_STACK"]?
  when nil, "", "0"
  when "1"
    abort "Error: LAVINMQ_EXPERIMENTAL_GC_GROW_MARK_STACK needs the patched libgc, build with make" unless GC::PATCHED
    GC.grow_mark_stack = true
    LavinMQ::Log.for("gc").info { "LAVINMQ_EXPERIMENTAL_GC_GROW_MARK_STACK=1: growing the GC mark stack" }
  else
    LavinMQ::Log.for("gc").warn { "Ignoring LAVINMQ_EXPERIMENTAL_GC_GROW_MARK_STACK=#{grow_mark_stack}, set it to 1 to switch it on" }
  end
  if config.raise_gc_warn?
    LibGC.set_warn_proc ->(msg, _word) {
      raise String.new(msg)
    }
  end
{% end %}
unless config.journald_stream? || config.log_file
  STDOUT.puts <<-LOGO

        ██╗      █████╗ ██╗   ██╗██╗███╗   ██╗███╗   ███╗ ██████╗
        ██║     ██╔══██╗██║   ██║██║████╗  ██║████╗ ████║██╔═══██╗
        ██║     ███████║██║   ██║██║██╔██╗ ██║██╔████╔██║██║   ██║
        ██║     ██╔══██║╚██╗ ██╔╝██║██║╚██╗██║██║╚██╔╝██║██║▄▄ ██║
        ███████╗██║  ██║ ╚████╔╝ ██║██║ ╚████║██║ ╚═╝ ██║╚██████╔╝
        ╚══════╝╚═╝  ╚═╝  ╚═══╝  ╚═╝╚═╝  ╚═══╝╚═╝     ╚═╝ ╚══▀▀═╝

                 The message broker built for peaks

    LOGO
end

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./lavinmq/launcher"
LavinMQ::Launcher.new(config).run
