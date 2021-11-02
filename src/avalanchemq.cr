require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "./avalanchemq/server_cli"
require "./avalanchemq/reporter"
require "./avalanchemq/launcher"

# TODO move more requires

config = AvalancheMQ::Config.instance
AvalancheMQ::ServerCLI.new(config).parse

if config.raise_gc_warn
  LibGC.set_warn_proc ->(msg, _word) {
    raise String.new(msg)
  }
end

# config has to be loaded before we require vhost/queue, byte_format is a constant
# TODO all these needed here?
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/log_formatter"

AvalancheMQ::Launcher.new(config) # will block
