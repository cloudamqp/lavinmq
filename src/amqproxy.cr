require "./amqproxy/version"
require "./amqproxy/server"
require "option_parser"

url = "amqp://guest:guest@localhost:5672"
port = 1234

OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-u AMQP_URL", "--upstream=AMQP_URL", "URL to upstream server") do |u|
    url = u
  end
  parser.on("-p PORT", "--port=PORT", "Port to listen on") { |p| port = p.to_i }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
end

server = AMQProxy::Server.new(url)
server.listen(port)
