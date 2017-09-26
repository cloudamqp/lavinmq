require "./amqpserver/version"
require "./amqpserver/server"
require "./amqpserver/http_server"
require "option_parser"
require "file"
require "ini"

port = 5672
config = ""

OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-c CONFIG_FILE", "--config=CONFIG_FILE", "Config file to read") do |c|
    config = c
  end
  parser.on("-p PORT", "--port=PORT", "Port to listen on") { |p| port = p.to_i }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end

unless config.empty?
  puts "Trying to read config #{config}"
  #abort "Config could not be found" unless File.file?(config)
  ini = INI.parse(File.read(config))
  p ini
end

amqp_server = AMQPServer::Server.new("/tmp")
spawn do
  amqp_server.listen(port)
end

http_server = AMQPServer::HTTPServer.new(amqp_server, 8080)
spawn do
  http_server.listen
end

sleep
