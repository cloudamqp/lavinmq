require "./lavinmq/journal_error_writer"
if ENV.has_key?("JOURNAL_STREAM")
  STDERR.reopen(LavinMQ::JournalErrorWriter.new)
end

require "./lavinmq/version"
require "./stdlib/*"
require "./lavinmq/config"

config = LavinMQ::Config.instance
config.parse # both ARGV and config file

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
