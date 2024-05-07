{% if flag?(:preview_mt) %}
  require "execution_context"
{% end %}
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
require "./lavinmq/clustering/controller"

if config.clustering?
  LavinMQ::Clustering::Controller.new(config).run
else
  LavinMQ::Launcher.new(config).run
end
