require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/log_formatter"
require "option_parser"
require "file"
require "ini"

config_file = ""
config = AvalancheMQ::Config.new

p = OptionParser.parse do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| config.data_dir = v }
  parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| config_file = v }
  parser.on("-p PORT", "--amqp-port=PORT", "AMQP port to listen on (default: 5672)") do |v|
    config.amqp_port = v.to_i
  end
  parser.on("--amqps-port=PORT", "AMQPS port to listen on (default: -1)") do |v|
    config.amqps_port = v.to_i
  end
  parser.on("--http-port=PORT", "HTTP port to listen on (default: 15672)") do |v|
    config.http_port = v.to_i
  end
  parser.on("--https-port=PORT", "HTTPS port to listen on (default: -1)") do |v|
    config.https_port = v.to_i
  end
  parser.on("--amqp-unix-path=PATH", "AMQP UNIX path to listen to") do |v|
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
puts "Pid: #{Process.pid}"

fd_limit = System.file_descriptor_limit
puts "FD limit: #{fd_limit}"
puts "The file descriptor limit is very low, consider raising it. You need one for each connection and two for each queue." if fd_limit < 1025

log = Logger.new(STDOUT, level: config.log_level.not_nil!)
AvalancheMQ::LogFormatter.use(log)
amqp_server = AvalancheMQ::Server.new(config.data_dir, log.dup)

if config.amqp_port > 0
  spawn(name: "AMQP listening on #{config.amqp_port}") do
    amqp_server.not_nil!.listen(config.amqp_bind, config.amqp_port)
  end
end

if config.amqps_port > 0 && !config.cert_path.empty?
  spawn(name: "AMQPS listening on #{config.amqps_port}") do
    amqp_server.not_nil!.listen_tls(config.amqp_bind, config.amqps_port,
                                    config.cert_path,
                                    config.key_path || config.cert_path)
  end
end

unless config.unix_path.empty?
  spawn(name: "AMQP listening at #{config.unix_path}") do
    amqp_server.not_nil!.listen_unix(config.unix_path)
  end
end

if config.http_port > 0 || config.https_port > 0
  http_server = AvalancheMQ::HTTP::Server.new(amqp_server, log.dup)
  if config.http_port > 0
    http_server.bind_tcp(config.http_bind, config.http_port)
  end
  if config.https_port > 0 && !config.cert_path.empty?
    http_server.bind_tls(config.http_bind, config.https_port,
                         config.cert_path,
                         config.key_path || config.cert_path)
  end
  spawn(name: "HTTP listener") do
    http_server.not_nil!.listen
  end
end

Signal::USR1.trap do
  puts "Uptime: #{amqp_server.uptime}s"
  puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
  puts System.resource_usage
  puts GC.prof_stats
end

Signal::USR2.trap do
  puts "Garbage collecting"
  GC.collect
end

Signal::HUP.trap do
  if config_file.empty?
    puts "No configuration file to reload"
  else
    puts "Reloading configuration file '#{config_file}'"
    config.parse(config_file)
  end
end

shutdown = ->(_s : Signal) do
  puts "Shutting down gracefully..."
  puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
  puts System.resource_usage
  puts GC.prof_stats
  http_server.try &.close
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
