require "./lavinmq/version"
require "./stdlib/*"
require "./lavinmq/config"

config = LavinMQ::Config.instance
begin
  config.parse # both ARGV and config file
rescue ex : OptionParser::Exception
  abort "Error: #{ex.message}\nTry '#{PROGRAM_NAME} --help' for more information."
end

{% unless flag?(:gc_none) %}
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
launcher =
  begin
    LavinMQ::Launcher.new(config)
  rescue ex : LavinMQ::Clustering::VR::Error
    # A misconfigured clustering roster (empty, malformed, or this node not in
    # it) surfaces here while building the Controller. Abort cleanly rather than
    # dumping a backtrace, matching the OptionParser handling above.
    abort "Error: #{ex.message}"
  end
launcher.run
