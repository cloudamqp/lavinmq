require "./lavinmqctl/*"

parser = LavinMQCtl::Parser.new
parser.parse

client = LavinMQCtl::Client.new(parser.options)
commands = LavinMQCtl::Commands.new(client, parser)

begin
  commands.run
ensure
  client.close
end
