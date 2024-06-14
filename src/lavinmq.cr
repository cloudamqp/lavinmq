require "./lavinmq/version"
require "./stdlib/*"
require "./lavinmq/config"
require "./lavinmq/server_cli"

config = LavinMQ::Config.instance
LavinMQ::ServerCLI.new(config).parse

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
