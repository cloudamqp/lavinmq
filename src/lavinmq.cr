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

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./lavinmq/launcher"
LavinMQ::Launcher.new(config).run
