require "./amqproxy/version"
require "./amqproxy/server"
require "option_parser"

upstream_address = "localhost"
upstream_port = 5672
port = 1234

OptionParser.parse! do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-u UPSTREAM:PORT", "--upsteam=UPSTREAM:PORT", "Upstream server address") do |u|
    upstream_address, upstream_port_s = u.split(":")
    upstream_port = upstream_port_s.to_i
  end
  parser.on("-p PORT", "--port=PORT", "Port to listen on") { |p| port = p.to_i }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
end

server = AMQProxy::Server.new(upstream_address, upstream_port)
server.listen(port)
