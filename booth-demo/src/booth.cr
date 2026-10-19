require "option_parser"
require "./booth/app"
require "./booth/web"

config_path = "booth.yml"
debug = false
OptionParser.parse do |parser|
  parser.banner = "Usage: booth [-c booth.yml]"
  parser.on("-c PATH", "--config=PATH", "Config file (default: booth.yml)") { |v| config_path = v }
  parser.on("-d", "--debug", "Log every reconnect") { debug = true }
  parser.on("-h", "--help", "Show this help") do
    puts parser
    exit
  end
end

Log.setup do |c|
  backend = Log::IOBackend.new
  c.bind "*", :warn, backend
  level = debug ? Log::Severity::Debug : Log::Severity::Info
  c.bind "booth.*", level, backend
  c.bind "booth", level, backend
  # Lost connections are the point of the demo, not errors
  c.bind "amqp.*", :none, backend
end

config = Booth::Config.load(config_path)
app = Booth::App.new(config)
app.start
Booth::Web.new(app).listen(config.listen_host, config.listen_port)
