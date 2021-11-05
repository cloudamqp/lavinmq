require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "./avalanchemq/server_cli"

config = AvalancheMQ::Config.instance
AvalancheMQ::ServerCLI.new(config).parse

if config.raise_gc_warn
  LibGC.set_warn_proc ->(msg, _word) {
    raise String.new(msg)
  }
end

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./avalanchemq/launcher"
AvalancheMQ::Launcher.new(config) # will block
