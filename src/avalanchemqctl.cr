require "http/client"
require "json"
require "option_parser"

options = {
  "host" => "http://127.0.0.1:8080"
} of String => String

parser = OptionParser.new
banner = parser.banner = "Usage: #{PROGRAM_NAME} [arguments] entity"
parser.on("-a data", "--add=data", "Create entity(queue, policy, etc.), data is json") do |a|
  options["add"] = a
end
parser.on("-r name", "--remove=name", "Remove entity(queue, policy, etc.)") do |r|
  options["remove"] = r
end
parser.on("-v vhost", "--vhost=vhost", "Specify vhost") do |v|
  options["vhost"] = v
end
parser.on("-H host", "--host=host", "Specify host. Default: #{options["host"]}") do |v|
  options["vhost"] = v
end
parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
parser.parse!

abort banner unless (entity = ARGV.shift?) && ["queues", "policies", "exchanges"].includes?(entity)
headers = HTTP::Headers{"Content-Type" => "application/json"}

begin
  if name = options["remove"]?
    resp = HTTP::Client.delete "#{options["host"]}/api/#{entity}", headers, name.to_s
    if resp.status_code == 200
      exit 0
    else
      puts resp.body
      exit 1
    end
  elsif data = options["add"]?
    resp = HTTP::Client.post "#{options["host"]}/api/#{entity}", headers, data.to_s
    if resp.status_code == 200
      exit 0
    else
      puts resp.body
      exit 1
    end
  else
    response = HTTP::Client.get "#{options["host"]}/api/#{entity}"
    abort "Response status #{response.status_code}" unless response.status_code == 200
    puts response.body
  end
rescue ex : IO::Error | Errno
  abort ex
end
