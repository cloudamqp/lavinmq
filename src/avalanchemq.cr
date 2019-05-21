require "./avalanchemq/version"
require "./avalanchemq/io_tweak"
require "./avalanchemq/stdlib_fixes"
require "./avalanchemq/resource"
require "./avalanchemq/config"
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/log_formatter"
require "option_parser"
require "file"
require "ini"

config_file = ""
config = AvalancheMQ::Config.new

p = OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| config.data_dir = v }
  parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| config_file = v }
  parser.on("-p PORT", "--port=PORT", "AMQP port to listen on (default: 5672)") do |v|
    config.port = v.to_i
  end
  parser.on("--tls-port=PORT", "AMQPS port to listen on (default: 5671)") do |v|
    config.tls_port = v.to_i
  end
  parser.on("--unix-path=PATH", "UNIX path to listen to") do |v|
    config.unix_path = v
  end
  parser.on("--cert FILE", "TLS certificate (including chain)") { |v| config.cert_path = v }
  parser.on("--key FILE", "Private key for the TLS certificate") { |v| config.key_path = v }
  parser.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |v|
    level = Logger::Severity.parse?(v.to_s)
    config.log_level = level if level
  end
  parser.on("-d", "--debug", "Verbose logging") { config.log_level = Logger::DEBUG }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end

config.parse(config_file) unless config_file.empty?

if config.data_dir.empty?
  STDERR.puts "No data directory specified"
  STDERR.puts p
  exit 2
end

puts "AvalancheMQ #{AvalancheMQ::VERSION}"

fd_limit = System.file_descriptor_limit
puts "FD limit: #{fd_limit}"
puts "The file descriptor limit is very low, consider raising it. You need one for each connection and two for each queue." if fd_limit < 1025

log = Logger.new(STDOUT, level: config.log_level.not_nil!)
AvalancheMQ::LogFormatter.use(log)
amqp_server = AvalancheMQ::Server.new(config.data_dir, log.dup)

if !config.cert_path.empty? && !config.key_path.empty?
  spawn(name: "AMQPS listening on #{config.tls_port}") do
    amqp_server.not_nil!.listen_tls(config.tls_port, config.cert_path, config.key_path)
  end
end

if !config.unix_path.empty?
  spawn(name: "AMQP listening #{config.unix_path}") do
    amqp_server.not_nil!.listen_unix(config.unix_path)
  end
end

spawn(name: "AMQP listening on #{config.port}") do
  amqp_server.not_nil!.listen(config.port)
end

http_server = AvalancheMQ::HTTP::Server.new(amqp_server, config.mgmt_port, log.dup)
spawn(name: "HTTP listener") do
  http_server.listen
end

Signal::HUP.trap do
  puts "Reloading"
  Fiber.list { |f| puts f.inspect }
  pp System.resource_usage
  pp GC.stats
  puts "Uptime: #{amqp_server.uptime}s"
end

shutdown = ->(_s : Signal) do
  puts "Shutting down gracefully..."
  http_server.close
  amqp_server.close
  Fiber.yield
  puts "Fibers: "
  Fiber.list { |f| puts f.inspect }
  exit 0
end
Signal::INT.trap &shutdown
Signal::TERM.trap &shutdown
GC.collect
sleep
