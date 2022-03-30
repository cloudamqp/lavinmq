require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "./avalanchemq/server_cli"
require "./avalanchemq/tls_proxy"

if ARGV[0]? == "tls-proxy"
  ARGV.shift
  config = AvalancheMQ::Config.instance
  AvalancheMQ::ServerCLI.new(config).parse
  AvalancheMQ::TLSProxy.new(config).run
  exit 0
end

config = AvalancheMQ::Config.instance
AvalancheMQ::ServerCLI.new(config).parse
{% unless flag?(:gc_none) %}
  if config.raise_gc_warn
    LibGC.set_warn_proc ->(msg, _word) {
      raise String.new(msg)
    }
  end
{% end %}

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./avalanchemq/launcher"
AvalancheMQ::Launcher.new(config).run # will block
