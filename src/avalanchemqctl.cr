require "http/client"
require "json"

USAGE = "Usage: #{File.basename PROGRAM_NAME} [queues | exchanges | connections]"
abort USAGE if ARGV.size != 1

begin
  case ARGV.first
  when "queues"
    response = HTTP::Client.get "http://127.0.0.1:8080/api/queues"
    abort "Response status #{response.status_code}" unless response.status_code == 200
    STDOUT << "Name\tMessages\n"
    queues = JSON.parse(response.body)
    queues.each do |q|
      STDOUT << q["name"]
      STDOUT << "\t"
      STDOUT << q["messages"]
      STDOUT << "\n"
    end
  else
    abort USAGE
  end
rescue ex : IO::Error | Errno
  abort ex
end
