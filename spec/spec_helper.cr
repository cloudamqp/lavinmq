require "log"
Log.setup_from_env(default_level: :error)

# Enable MT
count = Fiber::ExecutionContext.default_workers_count
Fiber::ExecutionContext.default.resize(count)

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
  wait_for { s.@listeners.shared(&.keys.select(TCPServer).first?) }.local_address.port
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
  begin
    if tls
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = "spec/resources/server_certificate.pem"
      ctx.private_key = "spec/resources/server_key.pem"
      spawn(name: "amqp tls listen") { s.listen_tls(tcp_server, ctx, LavinMQ::Server::Protocol::AMQP) }
    else
      spawn(name: "amqp tcp listen") { s.listen(tcp_server, LavinMQ::Server::Protocol::AMQP) }
    end
    wait_for { !s.listeners_empty? }
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
    s.close
  end
end

def with_http_server(file = __FILE__, line = __LINE__, &)
  with_amqp_server(file: file, line: line) do |s|
    h = LavinMQ::HTTP::Server.new(s)
    begin
      addr = h.bind_tcp("::1", ENV.has_key?("NATIVE_PORTS") ? 15672 : 0)
      spawn(name: "http listen") { h.listen }
      Fiber.yield
      yield({HTTPSpecHelper.new(addr), s})
    ensure
      h.close
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
