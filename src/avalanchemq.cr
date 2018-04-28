require "./avalanchemq/version"
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/stdlib_fixes"
require "option_parser"
require "file"
require "ini"

puts "AvalancheMQ #{AvalancheMQ::VERSION}"

log_level = Logger::INFO
port = 5672
tls_port = 5671
data_dir = ""
config = ""
cert_path = ""
key_path = ""

p = OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |d| data_dir = d }
  parser.on("-c CONF", "--config=CONF", "Path to config file") do |c|
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
  parser.on("-d", "--debug", "Verbose logging") { log_level = Logger::DEBUG }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end

unless config.empty?
  if File.file?(config)
    ini = INI.parse(File.read(config))
    port = ini["amqp"]["port"].to_i32
    data_dir = ini["amqp"]["data_dir"]
    log_level = Logger::DEBUG if ini["amqp"]["debug"]
  else
    abort "Config could not be found"
  end
end
if data_dir.empty?
  STDERR.puts "Path to data directory is required"
  STDERR.puts p
  exit 2
end

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

http_server = AvalancheMQ::HTTPServer.new(amqp_server, 15672)
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
