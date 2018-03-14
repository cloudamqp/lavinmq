require "./amqpserver/version"
require "./amqpserver/server"
require "./amqpserver/http_server"
require "option_parser"
require "file"
require "ini"

log_level = Logger::ERROR
port = 5672
data_dir = "/tmp"
config = ""

OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-c CONFIG_FILE", "--config=CONFIG_FILE", "Config file to read") do |c|
    config = c
  end
  parser.on("-p PORT", "--port=PORT", "Port to listen on") { |p| port = p.to_i }
  parser.on("-D DATA_DIR", "--data-dir=DATA_DIR", "Directory path to data") { |d| data_dir = d }
  parser.on("-d", "--debug", "Verbose logging") { log_level = Logger::DEBUG }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end
unless config.empty?
  puts "Trying to read config #{config}"
  #abort "Config could not be found" unless File.file?(config)
  ini = INI.parse(File.read(config))
  p ini
end

amqp_server = AMQPServer::Server.new(data_dir, log_level)
spawn do
  amqp_server.listen(port)
end

http_server = AMQPServer::HTTPServer.new(amqp_server, 8080)
spawn do
  http_server.listen
end

Signal::HUP.trap do
  puts "Reloading"
end
shutdown = -> (s : Signal) do
  print "Terminating..."
  http_server.close
  print "HTTP Done..."
  amqp_server.close
  print "AMQP Done!\n"
  exit 0
end
Signal::INT.trap &shutdown
Signal::TERM.trap &shutdown
sleep
