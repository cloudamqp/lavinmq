require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/log_formatter"
require "option_parser"
require "file"
require "ini"

config_file = ""
config = AvalancheMQ::Config.new

p = OptionParser.parse do |parser|
  parser.banner = "Usage: #{PROGRAM_NAME} [arguments]"
  parser.on("-D DATADIR", "--data-dir=DATADIR", "Data directory") { |v| config.data_dir = v }
  parser.on("-c CONF", "--config=CONF", "Config file (INI format)") { |v| config_file = v }
  parser.on("-p PORT", "--amqp-port=PORT", "AMQP port to listen on (default: 5672)") do |v|
    config.amqp_port = v.to_i
  end
  parser.on("--amqps-port=PORT", "AMQPS port to listen on (default: -1)") do |v|
    config.amqps_port = v.to_i
  end
  parser.on("--http-port=PORT", "HTTP port to listen on (default: 15672)") do |v|
    config.http_port = v.to_i
  end
  parser.on("--https-port=PORT", "HTTPS port to listen on (default: -1)") do |v|
    config.https_port = v.to_i
  end
  parser.on("--amqp-unix-path=PATH", "AMQP UNIX path to listen to") do |v|
    config.unix_path = v
  end
  parser.on("--cert FILE", "TLS certificate (including chain)") { |v| config.cert_path = v }
  parser.on("--key FILE", "Private key for the TLS certificate") { |v| config.key_path = v }
  parser.on("-l", "--log-level=LEVEL", "Log level (Default: info)") do |v|
    level = Logger::Severity.parse?(v.to_s)
    config.log_level = level if level
  end
  parser.on("-d", "--debug", "Verbose logging") { config.log_level = Logger::DEBUG }
  parser.on("-h", "--help", "Show this help") { puts parser; exit 1 }
  parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
  parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
end

config.parse(config_file) unless config_file.empty?

if config.data_dir.empty?
  STDERR.puts "No data directory specified"
  STDERR.puts p
  exit 2
end

puts "AvalancheMQ #{AvalancheMQ::VERSION}"
puts "Pid: #{Process.pid}"

fd_limit = System.file_descriptor_limit
puts "FD limit: #{fd_limit}"
puts "The file descriptor limit is very low, consider raising it. You need one for each connection and two for each queue." if fd_limit < 1025

log = Logger.new(STDOUT, level: config.log_level.not_nil!)
AvalancheMQ::LogFormatter.use(log)
amqp_server = AvalancheMQ::Server.new(config.data_dir, log.dup)

if config.amqp_port > 0
  spawn(name: "AMQP listening on #{config.amqp_port}") do
    amqp_server.not_nil!.listen(config.amqp_bind, config.amqp_port)
  end
end

if config.amqps_port > 0 && !config.cert_path.empty?
  spawn(name: "AMQPS listening on #{config.amqps_port}") do
    amqp_server.not_nil!.listen_tls(config.amqp_bind, config.amqps_port,
                                    config.cert_path,
                                    config.key_path || config.cert_path)
  end
end

unless config.unix_path.empty?
  spawn(name: "AMQP listening at #{config.unix_path}") do
    amqp_server.not_nil!.listen_unix(config.unix_path)
  end
end

if config.http_port > 0 || config.https_port > 0
  http_server = AvalancheMQ::HTTP::Server.new(amqp_server, log.dup)
  if config.http_port > 0
    http_server.bind_tcp(config.http_bind, config.http_port)
  end
  if config.https_port > 0 && !config.cert_path.empty?
    http_server.bind_tls(config.http_bind, config.https_port,
                         config.cert_path,
                         config.key_path || config.cert_path)
  end
  spawn(name: "HTTP listener") do
    http_server.not_nil!.listen
  end
end

macro puts_size_capacity(obj, indent = 0)
  STDOUT << " " * {{ indent }}
  STDOUT << "{{ obj.name }}"
  STDOUT << " size="
  STDOUT << {{obj}}.size
  STDOUT << " capacity="
  STDOUT << {{obj}}.capacity
  STDOUT << '\n'
end

def report(s)
  puts "Flow=#{s.flow?}"
  puts_size_capacity s.@connections
  s.connections.each do |c|
    puts "  #{c.name}"
    puts_size_capacity c.@channels, 4
    c.channels.each_value do |ch|
      puts "    #{ch.id} prefetch=#{ch.prefetch_size}"
      puts_size_capacity ch.@next_msg_body, 6
      puts_size_capacity ch.@unacked, 6
      puts_size_capacity ch.@consumers, 6
      ch.consumers.each do |cons|
        puts "      #{cons.tag}"
        puts_size_capacity cons.@unacked, 8
      end
    end
  end
  puts_size_capacity s.@users
  puts_size_capacity s.@vhosts
  s.vhosts.each do |_, vh|
    puts "VHost #{vh.name}"
    puts_size_capacity vh.@awaiting_confirm, 4
    puts_size_capacity vh.@cache, 4
    vh.@cache.each do |fiber, cache|
      puts "    #{fiber.inspect} match cache"
      puts_size_capacity cache[0], 6
      puts_size_capacity cache[1], 6
    end
    puts_size_capacity vh.@exchanges, 4
    puts_size_capacity vh.@queues, 4
    vh.queues.each do |_, q|
      puts "    #{q.name} #{q.durable ? "durable" : ""} args=#{q.arguments}"
      puts_size_capacity q.@segments, 6
      puts_size_capacity q.@segment_pos, 6
      puts_size_capacity q.@segment_ref_count, 6
      puts_size_capacity q.@consumers, 6
      puts_size_capacity q.@ready, 6
      puts_size_capacity q.@requeued, 6
      puts_size_capacity q.@deliveries, 6
      puts_size_capacity q.@get_unacked, 6
    end
  end
end

def dump_string_pool(io)
  pool = AMQ::Protocol::ShortString::POOL
  io.puts "# size=#{pool.size} capacity=#{pool.@capacity}"
  pool.@capacity.times do |i|
    str = pool.@values[i]
    next if str.empty?
    io.puts str
  end
end

Signal::USR1.trap do
  STDOUT.puts "Uptime: #{amqp_server.uptime}s"
  STDOUT.puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
  STDOUT.puts System.resource_usage
  STDOUT.puts GC.prof_stats
  report(amqp_server)
  File.open(File.join(amqp_server.data_dir, "string_pool.dump"), "w") do |f|
    STDOUT.puts "Dumping string pool to #{f.path}"
    dump_string_pool(f)
  end
  STDOUT.flush
end

Signal::USR2.trap do
  STDOUT.puts "Garbage collecting"
  STDOUT.flush
  GC.collect
  STDOUT.puts "Clearing string pool"
  STDOUT.flush
  AMQ::Protocol::ShortString::POOL.clear
end

Signal::HUP.trap do
  if config_file.empty?
    puts "No configuration file to reload"
  else
    puts "Reloading configuration file '#{config_file}'"
    config.parse(config_file)
  end
  STDOUT.flush
end

shutdown = ->(_s : Signal) do
  puts "Shutting down gracefully..."
  puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
  puts System.resource_usage
  puts GC.prof_stats
  http_server.try &.close
  amqp_server.close
  Fiber.yield
  puts "Fibers: "
  Fiber.list { |f| puts f.inspect }
  exit 0
end
Signal::INT.trap &shutdown
Signal::TERM.trap &shutdown
GC.collect
sleep
