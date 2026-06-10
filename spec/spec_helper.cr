require "log"
Log.setup_from_env(default_level: :error)

Signal::SEGV.reset # Let the OS generate a coredump

class Log
  def self.setup
    # noop, don't override during spec
  end

  def self.setup(level : Severity, backend : Backend)
    # noop, don't override during spec
  end
end

require "spec"
require "file_utils"
require "../src/lavinmq/config" # have to be required first
require "../src/lavinmq/server"
require "../src/lavinmq/amqp/server"
require "../src/lavinmq/mqtt/server"
require "../src/lavinmq/http/http_server"
require "../src/lavinmq/http/metrics_server"
require "http/client"
require "amqp-client"
require "./support/*"

# Applies the spec config defaults. Inherits the current per-example data dir
# (set by the `around_each` hook below) rather than hardcoding one, so a custom
# Config passed to `with_amqp_server`/mtls still ends up in the same dir that
# the hook created and cleans up.
def init_config(config = LavinMQ::Config.instance)
  config.data_dir = LavinMQ::Config.instance.data_dir
  config.segment_size = 512 * 1024
  config.consumer_timeout_loop_interval = 1
  config
end

LavinMQ::Config.instance.data_dir = "/tmp/lavinmq-spec"
init_config

# Allow creating custom config objects for specs
module LavinMQ
  class Config
    def initialize(@io : IO = STDERR)
    end

    def self.instance=(instance)
      @@instance = instance
    end
  end
end

class LavinMQ::Server
  def restart_stores_for_specs
    stop
    Dir.mkdir_p @data_dir
    LavinMQ::Schema.migrate(@data_dir, @replicator)
    @persister = LavinMQ::Persister.new(@data_dir)
    @users = LavinMQ::Auth::UserStore.new(@data_dir, @replicator)
    @authenticator = LavinMQ::Auth::Chain.create(@config, @users)
    @vhosts = LavinMQ::VHostStore.new(@data_dir, @users, @replicator, @persister)
    @parameters = LavinMQ::ParameterStore(LavinMQ::Parameter).new(@data_dir, "parameters.json", @replicator)
    apply_parameter
    start_log_exchange
    @closed.set(false)
    Fiber.yield
  end
end

SPEC_AMQP_SERVERS = {} of LavinMQ::Server => LavinMQ::AMQP::Server
SPEC_MQTT_SERVERS = {} of LavinMQ::Server => LavinMQ::MQTT::Server

def register_amqp(server : LavinMQ::Server, amqp_server : LavinMQ::AMQP::Server) : LavinMQ::AMQP::Server
  SPEC_AMQP_SERVERS[server] = amqp_server
end

def register_mqtt(server : LavinMQ::Server, mqtt_server : LavinMQ::MQTT::Server) : LavinMQ::MQTT::Server
  SPEC_MQTT_SERVERS[server] = mqtt_server
end

def unregister_amqp(server : LavinMQ::Server) : Nil
  SPEC_AMQP_SERVERS.delete(server)
end

def unregister_mqtt(server : LavinMQ::Server) : Nil
  SPEC_MQTT_SERVERS.delete(server)
end

def amqp(server : LavinMQ::Server) : LavinMQ::AMQP::Server
  SPEC_AMQP_SERVERS[server]
end

def mqtt(server : LavinMQ::Server) : LavinMQ::MQTT::Server
  SPEC_MQTT_SERVERS[server]
end

private def protocol_server_state(server : LavinMQ::ProtocolServer)
  tcp_listener = server.@listeners.select(TCPServer).first?
  {
    config:      server.@config,
    address:     tcp_listener.try(&.local_address.address),
    port:        tcp_listener.try(&.local_address.port),
    tls_context: tcp_listener.try { |listener| server.@tls_contexts[listener]? },
    listening:   server.listening?,
  }
end

private def restore_protocol_listener(server : LavinMQ::ProtocolServer, state, name : String) : Nil
  address = state[:address] || return
  port = state[:port] || return

  if tls_context = state[:tls_context]
    server.bind_tls(address, port, tls_context)
  else
    server.bind_tcp(address, port)
  end
  spawn(name: name) { server.listen } if state[:listening]
end

def restart_server(server : LavinMQ::Server)
  amqp_server = SPEC_AMQP_SERVERS[server]?
  mqtt_server = SPEC_MQTT_SERVERS[server]?
  amqp_state = amqp_server.try { |amqp| protocol_server_state(amqp) }
  mqtt_state = mqtt_server.try { |mqtt| protocol_server_state(mqtt) }
  amqp_server.try &.close
  mqtt_server.try &.close
  unregister_amqp(server)
  unregister_mqtt(server)

  server.restart_stores_for_specs

  if amqp_state
    amqp_server = register_amqp(server, LavinMQ::AMQP::Server.new(server, amqp_state[:config]))
    restore_protocol_listener(amqp_server, amqp_state, "amqp listener")
  end
  if mqtt_state
    mqtt_server = register_mqtt(server, LavinMQ::MQTT::Server.new(server, mqtt_state[:config]))
    restore_protocol_listener(mqtt_server, mqtt_state, "mqtt listener")
  end
  Fiber.yield
end

def with_datadir(&)
  data_dir = File.tempname("lavinmq", "spec")
  Dir.mkdir_p data_dir
  yield data_dir
ensure
  FileUtils.rm_rf data_dir if data_dir
end

def with_channel(s : LavinMQ::Server, file = __FILE__, line = __LINE__, **args, &)
  name = "lavinmq-spec-#{file}:#{line}"
  args = {port: amqp_port(s), name: name}.merge(args)
  conn = AMQP::Client.new(**args).connect
  ch = conn.channel
  yield ch
ensure
  conn.try &.close(no_wait: false)
end

def amqp_port(s)
  amqp(s).@listeners.select(TCPServer).first.local_address.port
end

# Poll interval for the wait_for/should_eventually loops below. We sleep
# (rather than busy-spinning with Fiber.yield) so the polling fiber blocks on
# an event-loop timer instead of re-enqueueing itself every round. A tight
# `loop { Fiber.yield }` keeps the run queue non-empty, so the event loop is
# only ever polled non-blocking while the CPU spins at 100%; on slower/contended
# runners (e.g. macOS CI) that starves the IO-bound server fibers and makes the
# work we're waiting for crawl past the timeout. Sleeping frees the CPU for them.
private WAIT_FOR_INTERVAL = 1.millisecond

# Default timeout for the wait_for/should_eventually polling helpers. Matched to
# the per-example timeout (SPEC_TIMEOUT) so the example-level timeout is the real
# cap on a stuck spec rather than a second, tighter, less-informative deadline
# that fires first and turns legitimately-slow-but-correct work on a loaded
# runner (e.g. macOS CI re-establishing connections) into spurious "Execution
# expired" failures. Polling returns as soon as the condition holds, so this
# costs nothing on success. "slow"-tagged examples that genuinely need to wait
# longer use explicit sleeps/timeouts, not this default. Pass an explicit,
# shorter timeout only when *not* observing something within a bound is the test.
private WAIT_FOR_TIMEOUT = 15.seconds

def should_eventually(expectation, timeout = WAIT_FOR_TIMEOUT, file = __FILE__, line = __LINE__, &)
  sec = Time.instant
  loop do
    sleep WAIT_FOR_INTERVAL
    begin
      yield.should(expectation, file: file, line: line)
      return
    rescue ex
      raise ex if Time.instant - sec > timeout
    end
  end
end

def wait_for(timeout = WAIT_FOR_TIMEOUT, file = __FILE__, line = __LINE__, &)
  sec = Time.instant
  loop do
    sleep WAIT_FOR_INTERVAL
    res = yield
    return res if res
    break if Time.instant - sec > timeout
  end
  fail "Execution expired", file: file, line: line
end

def with_amqp_server(tls = false, replicator = nil,
                     config = LavinMQ::Config.instance,
                     file = __FILE__, line = __LINE__, & : LavinMQ::Server -> Nil)
  LavinMQ::Config.instance = init_config(config)
  tcp_server = TCPServer.new("localhost", ENV.has_key?("NATIVE_PORTS") ? 5672 : 0)
  s = LavinMQ::Server.new(config, replicator)
  amqp_server = LavinMQ::AMQP::Server.new(s, config)
  register_amqp(s, amqp_server)
  begin
    if tls
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"
      amqp_server.bind_tls(tcp_server, ctx)
    else
      amqp_server.bind_tcp(tcp_server)
    end
    spawn(name: "amqp listener") { amqp_server.listen }
    Fiber.yield
    yield s
  ensure
    # A closed queue indicates that something failed that shouldn't. However, the error may be
    # silent and not causing a spec to fail. It's not even sure that the failing queue is related
    # to what the spec tests, but by failing on closed queues we may find bugs.
    # If a queue is supposed to be closed, it should be deleted in the end of the spec.

    # We yield a few times (if necessary) to make sure things has really settled, e.g.
    # everything has been cleaned up after a `with_channel` inside the `with_amqp_server`.
    closed_queues = 3.times do
      Fiber.yield
      queues = s.vhosts.flat_map { |_, vhost| vhost.queues.select &.closed? }
      break if queues.empty?
      queues
    end
    if closed_queues && !closed_queues.empty?
      msg = "Closed queues detected: #{closed_queues.map(&.name).join(", ")}\n\n" \
            "If they should be closed, please delete them in the end of the spec."
      raise Spec::AssertionFailed.new(msg, file, line)
    end
    amqp(s).close
    unregister_amqp(s)
    s.close
  end
end

def with_http_server(file = __FILE__, line = __LINE__, &)
  with_amqp_server(file: file, line: line) do |s|
    mqtt_server = LavinMQ::MQTT::Server.new(s)
    register_mqtt(s, mqtt_server)
    h = LavinMQ::HTTP::Server.new(s, amqp(s), mqtt_server)
    begin
      addr = h.bind_tcp("::1", ENV.has_key?("NATIVE_PORTS") ? 15672 : 0)
      spawn(name: "http listen") { h.listen }
      Fiber.yield
      yield({HTTPSpecHelper.new(addr), s})
    ensure
      h.close
      mqtt(s).close
      unregister_mqtt(s)
    end
  end
end

def serve_metrics(amqp_server, &)
  h = LavinMQ::HTTP::MetricsServer.new(amqp_server)
  begin
    addr = h.bind_tcp("::1", ENV.has_key?("NATIVE_PORTS") ? 15692 : 0)
    spawn(name: "metrics listen") { h.listen }
    Fiber.yield
    yield HTTPSpecHelper.new(addr)
  ensure
    h.close
  end
end

def with_metrics_server(file = __FILE__, line = __LINE__, &)
  with_amqp_server(file: file, line: line) do |s|
    serve_metrics(s) do |http|
      yield({http, s})
    end
  end
end

def with_follower_metrics_server(&)
  # Passing nil for amqp_server wires up FollowerPrometheusController — the same path a real follower takes.
  serve_metrics(nil) do |http|
    yield http
  end
end

struct HTTPSpecHelper
  def initialize(@addr : Socket::IPAddress)
  end

  getter addr

  def test_headers(headers : NamedTuple)
    hash = Hash(String, String).new(initial_capacity: headers.size)
    headers.each { |k, v| hash[k.to_s] = v }
    test_headers(hash)
  end

  def test_headers(headers = nil)
    req_hdrs = HTTP::Headers{"Content-Type"  => "application/json",
                             "Authorization" => "Basic Z3Vlc3Q6Z3Vlc3Q="} # guest:guest
    req_hdrs.merge!(headers) if headers
    req_hdrs
  end

  def test_uri(path)
    "http://#{@addr}#{path}"
  end

  def get(path, headers = nil)
    HTTP::Client.get(test_uri(path), headers: test_headers(headers))
  end

  def post(path, headers = nil, body = nil)
    HTTP::Client.post(test_uri(path), headers: test_headers(headers), body: body)
  end

  def put(path, headers = nil, body = nil)
    HTTP::Client.put(test_uri(path), headers: test_headers(headers), body: body)
  end

  def delete(path, headers = nil, body = nil)
    HTTP::Client.delete(test_uri(path), headers: test_headers(headers), body: body)
  end

  def exec(request : HTTP::Request)
    HTTP::Client.new(URI.parse "http://#{@addr}").exec(request)
  end
end

# Helper function for creating a queue with ttl and an associated dlq
def create_ttl_and_dl_queues(channel, queue_ttl = 1)
  args = AMQP::Client::Arguments.new
  args["x-message-ttl"] = queue_ttl
  args["x-dead-letter-exchange"] = ""
  args["x-dead-letter-routing-key"] = "dlq"
  q = channel.queue("ttl", args: args)
  dlq = channel.queue("dlq")
  {q, dlq}
end

def exit(status : Int32 | Process::Status = 0) : NoReturn
  raise SpecExit.new(status.is_a?(Int32) ? status : status.exit_code)
end

class SpecExit < Exception
  getter code : Int32

  def initialize(@code)
    super "Exiting with code #{@code}"
  end
end

private SPEC_TIMEOUT = 15.seconds

Spec.around_each do |example|
  done = Channel(Exception?).new

  spawn(name: "Spec: #{example.example.description}") do
    begin
      example.run
    rescue e
      done.send(e)
    else
      done.send(nil)
    end
  end

  timeout = SPEC_TIMEOUT
  if example.example.all_tags.includes?("slow")
    timeout *= 4
  end

  select
  when res = done.receive
    raise res if res
  when timeout(timeout)
    _it = example.example
    ex = Spec::AssertionFailed.new("spec timed out after #{timeout}", _it.file, _it.line)
    _it.parent.report(:fail, _it.description, _it.file, _it.line, timeout, ex)
  end
end

# Give every example a fresh Config (so config tweaks don't leak between
# examples) with its own data dir. around_each hooks stack (they don't
# override) and nest in registration order, so this runs *inside* the timeout
# hook above: `example.run` here is the actual example. On a timeout the
# example fiber is abandoned mid-run, so this block never returns and the
# `ensure` cleanup is skipped — leaving the still-running server writing into
# its own abandoned dir instead of colliding with the next example's dir
# (which previously caused "Invalid memory access"). On normal completion the
# dir is removed.
Spec.around_each do |example|
  data_dir = File.tempname("lavinmq", "spec")
  Dir.mkdir_p data_dir
  config = init_config(LavinMQ::Config.new)
  config.data_dir = data_dir
  LavinMQ::Config.instance = config
  begin
    example.run
  ensure
    FileUtils.rm_rf data_dir
  end
end
