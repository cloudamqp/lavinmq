require "./avalanchemq/version"
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/stdlib_fixes"
require "option_parser"
require "file"
require "ini"

config = ""
data_dir = ""
log_level = Logger::INFO
bind = "::"
port = 5672
tls_port = 5671
cert_path = ""
key_path = ""
mgmt_bind = "::"
mgmt_port = 15672
mgmt_tls_port = 15671
mgmt_cert_path = ""
mgmt_key_path = ""

p = OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |d| data_dir = d }
  parser.on("-c CONF", "--config=CONF", "Config file (INI format)") do |c|
    config = c
  end
  parser.on("-p PORT", "--port=PORT", "AMQP port to listen on (default: 5672)") do |p|
    port = p.to_i
  end
  parser.on("--tls-port=PORT", "AMQPS port to listen on (default: 5671)") do |p|
    tls_port = p.to_i
  end
  parser.on("--cert FILE", "TLS certificate (including chain)") do |f|
    cert_path = f
  end
  parser.on("--key FILE", "Private key for the TLS certificate") do |f|
    key_path = f
  end
  parser.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |l|
    log_level = Logger::Severity.parse(l)
  end
  parser.on("-d", "--debug", "Verbose logging") { log_level = Logger::DEBUG }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end

unless config.empty?
  if File.file?(config)
    ini = INI.parse(File.read(config))
    if main = ini["main"]
      data_dir = main["data_dir"] if main.has_key? "data_dir"
      log_level = Logger::Severity.parse(main["log_level"]) if main.has_key? "log_level"
    end
    if amqp = ini["amqp"]
      bind = amqp["bind"] if amqp.has_key? "bind"
      port = amqp["port"].to_i32 if amqp.has_key? "port"
      tls_port = amqp["tls_port"].to_i32 if amqp.has_key? "tls_port"
      cert_path = amqp["tls_cert"] if amqp.has_key? "tls_cert"
      key_path = amqp["tls_key"] if amqp.has_key? "tls_key"
    end
    if mgmt = ini["mgmt"]
      mgmt_bind = mgmt["bind"] if mgmt.has_key? "bind"
      mgmt_port = mgmt["port"].to_i32 if mgmt.has_key? "port"
      mgmt_tls_port = mgmt["tls_port"].to_i32 if mgmt.has_key? "tls_port"
      mgmt_cert_path = mgmt["tls_cert"] if mgmt.has_key? "tls_cert"
      mgmt_key_path = mgmt["tls_key"] if mgmt.has_key? "tls_key"
    end
  else
    abort "Config could not be found"
  end
end
if data_dir.empty?
  STDERR.puts "No data directory specified"
  STDERR.puts p
  exit 2
end

puts "AvalancheMQ #{AvalancheMQ::VERSION}"

fd_limit = `ulimit -n`.to_i
puts "FD limit: #{fd_limit}"
puts "The file descriptor limit is very low, consider raising it. You need one for each connection and two for each queue." if fd_limit < 1025

amqp_server = AvalancheMQ::Server.new(data_dir, log_level)
spawn(name: "AMQP listening on #{port}") do
  amqp_server.listen(port)
end

if !cert_path.empty? && !key_path.empty?
  spawn(name: "AMQPS listening on #{port}") do
    amqp_server.listen_tls(tls_port, cert_path, key_path)
  end
end

http_server = AvalancheMQ::HTTPServer.new(amqp_server, mgmt_port)
spawn(name: "HTTP listener") do
  http_server.listen
end

Signal::HUP.trap do
  puts "Reloading"
  Fiber.list { |f| puts f.inspect }
end
shutdown = -> (s : Signal) do
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
sleep
