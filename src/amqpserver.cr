require "./amqpserver/version"
require "./amqpserver/server"
require "option_parser"
require "file"
require "ini"

port = 1234
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
server = AMQPServer::Server.new
server.listen(port)
