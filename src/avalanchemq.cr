require "./avalanchemq/version"
require "./stdlib/*"
require "./avalanchemq/config"
require "file"
require "systemd"
require "./avalanchemq/server_cli"

config_file = ""
config = AvalancheMQ::Config.instance

AvalancheMQ::ServerCLI.new(config, config_file).parse

# config has to be loaded before we require vhost/queue, byte_format is a constant
require "./avalanchemq/server"
require "./avalanchemq/http/http_server"
require "./avalanchemq/log_formatter"

puts "AvalancheMQ #{AvalancheMQ::VERSION}"
{% unless flag?(:release) %}
  puts "WARNING: Not built in release mode"
{% end %}
{% if flag?(:preview_mt) %}
  puts "Multithreading: #{ENV.fetch("CRYSTAL_WORKERS", "4")} threads"
{% end %}
puts "Pid: #{Process.pid}"
puts "Data directory: #{config.data_dir}"

# Maximize FD limit
_, fd_limit_max = System.file_descriptor_limit
System.file_descriptor_limit = fd_limit_max
fd_limit_current, _ = System.file_descriptor_limit
puts "FD limit: #{fd_limit_current}"
if fd_limit_current < 1025
  puts "WARNING: The file descriptor limit is very low, consider raising it."
  puts "WARNING: You need one for each connection and two for each durable queue, and some more."
end

# Make sure that only one instance is using the data directory
# Can work as a poor mans cluster where the master nodes aquires
# a file lock on a shared file system like NFS
Dir.mkdir_p config.data_dir
lock = File.open(File.join(config.data_dir, ".lock"), "w+")
lock.sync = true
lock.read_buffering = false
begin
  lock.flock_exclusive(blocking: false)
rescue
  puts "INFO: Data directory locked by '#{lock.gets_to_end}'"
  puts "INFO: Waiting for file lock to be released"
  lock.flock_exclusive(blocking: true)
  puts "INFO: Lock aquired"
end
lock.truncate
lock.print System.hostname
lock.fsync

log = Logger.new(STDOUT, level: config.log_level.not_nil!)
AvalancheMQ::LogFormatter.use(log)
amqp_server = AvalancheMQ::Server.new(config.data_dir, log.dup)
http_server = AvalancheMQ::HTTP::Server.new(amqp_server, log.dup)

def reload_tls(context, config, log)
  if tls = context
    case config.tls_min_version
    when "1.0"
      tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 |
                         OpenSSL::SSL::Options::NO_TLS_V1_1 |
                         OpenSSL::SSL::Options::NO_TLS_V1)
      tls.ciphers = OpenSSL::SSL::Context::CIPHERS_OLD + ":@SECLEVEL=1"
    when "1.1"
      tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2 | OpenSSL::SSL::Options::NO_TLS_V1_1)
      tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1)
      tls.ciphers = OpenSSL::SSL::Context::CIPHERS_OLD + ":@SECLEVEL=1"
    when "1.2", ""
      tls.remove_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
      tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_1 | OpenSSL::SSL::Options::NO_TLS_V1)
      tls.ciphers = OpenSSL::SSL::Context::CIPHERS_INTERMEDIATE
    when "1.3"
      tls.add_options(OpenSSL::SSL::Options::NO_TLS_V1_2)
      tls.ciphers = OpenSSL::SSL::Context::CIPHERS_INTERMEDIATE
    else
      log.warn { "Unrecognized config value for tls_min_version: '#{config.tls_min_version}'" }
    end
    tls.certificate_chain = config.tls_cert_path
    tls.private_key = config.tls_key_path.empty? ? config.tls_cert_path : config.tls_key_path
    tls.ciphers = config.tls_ciphers unless config.tls_ciphers.empty?
  end
end

context = nil
if !config.tls_cert_path.empty?
  context = OpenSSL::SSL::Context::Server.new
  context.add_options(OpenSSL::SSL::Options.new(0x40000000)) # disable client initiated renegotiation
  reload_tls(context, config, log)
end

SystemD.listen_fds_with_names.each do |fd, name|
  case name
  when "avalanchemq.socket", "unknown"
    case
    when SystemD.is_tcp_listener?(fd)
      spawn amqp_server.listen(TCPServer.new(fd: fd)), name: "AMQP server listener"
    when SystemD.is_unix_stream_listener?(fd)
      spawn amqp_server.listen(UNIXServer.new(fd: fd)), name: "AMQP server listener"
    else
      raise "Unsupported amqp socket type"
    end
  when "avalanchemq-http.socket"
    case
    when SystemD.is_tcp_listener?(fd)
      http_server.bind(TCPServer.new(fd: fd))
    when SystemD.is_unix_stream_listener?(fd)
      http_server.bind(UNIXServer.new(fd: fd))
    else
      raise "Unsupported http socket type"
    end
  else
    puts "unexpected socket via systemd #{fd} '#{name}'"
  end
end

if config.amqp_port > 0
  spawn amqp_server.listen(config.amqp_bind, config.amqp_port),
    name: "AMQP listening on #{config.amqp_port}"
end

if config.amqps_port > 0
  if ctx = context
    spawn amqp_server.listen_tls(config.amqp_bind, config.amqps_port, ctx),
      name: "AMQPS listening on #{config.amqps_port}"
  else
    log.warn { "Certificate for AMQPS not configured" }
  end
end

unless config.unix_path.empty?
  spawn amqp_server.listen_unix(config.unix_path), name: "AMQP listening at #{config.unix_path}"
end

if config.http_port > 0
  http_server.bind_tcp(config.http_bind, config.http_port)
end
if config.https_port > 0
  if ctx = context
    http_server.bind_tls(config.http_bind, config.https_port, ctx)
  end
end
unless config.http_unix_path.empty?
  http_server.bind_unix(config.http_unix_path)
end
http_server.bind_internal_unix
spawn(name: "HTTP listener") do
  http_server.not_nil!.listen
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
  puts_size_capacity s.@users
  puts_size_capacity s.@vhosts
  s.vhosts.each do |_, vh|
    puts "VHost #{vh.name}"
    puts_size_capacity vh.@awaiting_confirm, 4
    puts_size_capacity vh.@exchanges, 4
    puts_size_capacity vh.@queues, 4
    puts_size_capacity vh.@segment_holes, 4
    vh.queues.each do |_, q|
      puts "    #{q.name} #{q.durable ? "durable" : ""} args=#{q.arguments}"
      puts_size_capacity q.@consumers, 6
      puts_size_capacity q.@ready, 6
      puts_size_capacity q.@unacked, 6
      puts_size_capacity q.@requeued, 6
      puts_size_capacity q.@deliveries, 6
    end
    puts_size_capacity vh.@connections
    vh.connections.each do |c|
      puts "  #{c.name}"
      puts_size_capacity c.@channels, 4
      c.channels.each_value do |ch|
        puts "    #{ch.id} prefetch=#{ch.prefetch_size}"
        puts_size_capacity ch.@unacked, 6
        puts_size_capacity ch.@consumers, 6
        puts_size_capacity ch.@visited, 6
        puts_size_capacity ch.@found_queues, 6
      end
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
  STDOUT.puts System.resource_usage
  STDOUT.puts GC.prof_stats
  fcount = 0
  Fiber.list { fcount += 1 }
  puts "Fiber count: #{fcount}"
  report(amqp_server)
  STDOUT.puts "String pool size: #{AMQ::Protocol::ShortString::POOL.size}"
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
end

Signal::HUP.trap do
  SystemD.notify("RELOADING=1\n")
  if config_file.empty?
    log.info { "No configuration file to reload" }
  else
    log.info { "Reloading configuration file '#{config_file}'" }
    config.parse(config_file)
    reload_tls(context, config, log)
  end
  SystemD.notify("READY=1\n")
end

first_shutdown_attempt = true
shutdown = ->(_s : Signal) do
  if first_shutdown_attempt
    first_shutdown_attempt = false
    SystemD.notify("STOPPING=1\n")
    puts "Shutting down gracefully..."
    amqp_server.close
    http_server.try &.close
    puts "Fibers: "
    Fiber.yield
    Fiber.list { |f| puts f.inspect }
    lock.close
    exit 0
  else
    puts "Fibers: "
    Fiber.list { |f| puts f.inspect }
    exit 1
  end
end
Signal::INT.trap &shutdown
Signal::TERM.trap &shutdown
SystemD.notify("READY=1\n")
GC.collect

# write to the lock file to detect lost lock
# See "Lost locks" in `man 2 fcntl`
begin
  hostname = System.hostname.to_slice
  loop do
    sleep 30
    lock.write_at hostname, 0
  end
rescue ex : IO::Error
  STDERR.puts ex.inspect
  abort "ERROR: Lost lock!"
end
