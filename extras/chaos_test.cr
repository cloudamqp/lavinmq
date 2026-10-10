# Jepsen-style chaos test for a LavinMQ raft cluster.
#
# Starts NODES nodes on 127.0.0.1, 127.0.0.2, ... (on macOS add the aliases
# first: `sudo ifconfig lo0 alias 127.0.0.2 up`). Publishers with publisher
# confirms and consumers with manual acks connect to random nodes, so
# followers proxy them to the leader, while a nemesis transfers leadership,
# kills, pauses and restarts nodes. Then it heals the cluster, drains the
# queues and checks that:
# - every confirmed message was delivered, and is still in the stream
# - nothing was delivered that wasn't published
# - no node exited unexpectedly or logged a crash
#
# Usage:
#   make bin/lavinmq bin/chaos-test
#   bin/chaos-test
#
# Configured by environment variables, see the constants below, e.g.:
#   DURATION=300 OPS=transfer:1 SEED=1234 bin/chaos-test
# OPS are nemesis operations with weights: transfer, kill_leader,
# pause_leader (SIGSTOP), kill_follower and restart_all (kill -9 every node).
# The nodes' data dirs and logs are kept in CHAOS_DIR, which is emptied
# when it starts.
require "amqp-client"
require "file_utils"
require "http/client"
require "json"

# EXTRA_CONFIG is added to the nodes' [main] config, e.g. "segment_size =
# 536870912" keeps a run's messages in one segment, so a lost message's
# neighbours are still on disk afterwards. CONSUME_DURING=0 starts the
# consumers only once the nemesis is done, telling a loss when publishing
# apart from one when consuming.
BIN            = ENV.fetch("LAVINMQ_BIN", "bin/lavinmq")
DIR            = ENV.fetch("CHAOS_DIR", "tmp/chaos")
NODES          = ENV.fetch("NODES", "3").to_i
DURATION       = ENV.fetch("DURATION", "120").to_i.seconds
INTERVAL       = ENV.fetch("INTERVAL", "4").to_f.seconds
OPS            = ENV.fetch("OPS", "transfer:6,kill_leader:1,pause_leader:1,kill_follower:1,restart_all:0").split(',').map { |kv| k, v = kv.split(':'); {k, v.to_i} }
QUEUES         = ENV.fetch("QUEUES", "chaos-classic:classic,chaos-stream:stream").split(',').map { |kv| k, v = kv.split(':'); {k, v} }
PUBS           = ENV.fetch("PUBLISHERS", "4").to_i
INFLIGHT       = ENV.fetch("INFLIGHT", "50").to_i
ELECTION       = ENV.fetch("ELECTION_MS", "1000")
HEARTBEAT      = ENV.fetch("HEARTBEAT_MS", "100")
CONSUME_DURING = ENV.fetch("CONSUME_DURING", "1") == "1"
SEED           = ENV.fetch("SEED", Random.new.rand(1_000_000).to_s).to_i

RNG = Random.new(SEED)

def log(msg)
  STDOUT.puts "#{Time.utc.to_s("%H:%M:%S.%L")} #{msg}"
  STDOUT.flush
end

def ip(i)
  "127.0.0.#{i + 1}"
end

class Node
  getter i : Int32
  getter process : Process?
  @expected_exit = false
  getter crashes = [] of String
  @log : File

  def initialize(@i)
    Dir.mkdir_p(data_dir)
    @log = File.open(log_path, "a")
  end

  def data_dir
    File.join(DIR, "node#{@i + 1}")
  end

  def log_path
    File.join(DIR, "node#{@i + 1}.log")
  end

  def running?
    @process.try { |p| !p.terminated? } || false
  end

  def start
    return if running?
    seeds = (0...NODES).map { |n| ip(n) }.join(',')
    args = [
      "--data-dir=#{data_dir}", "--bind=#{ip(@i)}", "--metrics-http-bind=#{ip(@i)}",
      "--control-unix-path=#{File.join(DIR, "ctl#{@i + 1}.sock")}",
      "--clustering", "--clustering-bind=#{ip(@i)}", "--clustering-password=chaos",
      "--clustering-seeds=#{seeds}", "--clustering-election-timeout=#{ELECTION}",
      "--clustering-heartbeat-interval=#{HEARTBEAT}",
    ]
    args << "--clustering-bootstrap" if @i == 0
    args << "--config=#{CONFIG}"
    @log.puts "===== #{Time.utc} starting"
    @log.flush
    @expected_exit = false
    p = Process.new(BIN, args, output: @log, error: @log)
    @process = p
    spawn(name: "watch node #{@i + 1}") do
      status = p.wait
      next if @process != p
      unless @expected_exit
        msg = "node#{@i + 1} exited unexpectedly: #{status.inspect}"
        log "!!! #{msg}"
        @crashes << "#{Time.utc.to_s("%H:%M:%S.%L")} #{msg}"
        # Like systemd's Restart=on-failure
        sleep 1.second
        start if @process == p
      end
    end
  end

  def signal(sig : Signal, expected = true)
    if p = @process
      @expected_exit = true if expected && sig.kill?
      p.signal(sig) rescue nil
    end
  end

  def kill
    signal(Signal::KILL)
    @process.try &.wait rescue nil
  end

  def stop
    @expected_exit = true
    if p = @process
      p.signal(Signal::TERM) rescue nil
      deadline = Time.instant + 30.seconds
      until p.terminated? || Time.instant > deadline
        sleep 100.milliseconds
      end
      unless p.terminated?
        log "node#{@i + 1} didn't stop in 30s, killing"
        p.signal(Signal::KILL) rescue nil
      end
    end
  end
end

FileUtils.rm_rf(DIR)
Dir.mkdir_p(DIR)
# The guest user may only connect over loopback, which a connection proxied
# by a follower isn't
CONFIG    = File.join(DIR, "lavinmq.ini")
File.write(CONFIG, "[main]\ndefault_user_only_loopback = false\n#{ENV["EXTRA_CONFIG"]?}\n")
NODE_LIST = (0...NODES).map { |i| Node.new(i) }

def http(i, method, path, body = nil)
  client = HTTP::Client.new(ip(i), 15672)
  client.connect_timeout = 2.seconds
  client.read_timeout = 5.seconds
  client.basic_auth("guest", "guest")
  headers = HTTP::Headers{"Content-Type" => "application/json"}
  client.exec(method, path, headers, body)
ensure
  client.try &.close
end

# The index of the node that leads, according to any node that answers
def leader : Int32?
  NODE_LIST.shuffle(RNG).each do |n|
    next unless n.running?
    resp = http(n.i, "GET", "/api/cluster") rescue next
    next unless resp.status_code == 200
    addr = JSON.parse(resp.body)["leader"]?.try(&.as_s?) || next
    host = addr.split(':').first
    return (0...NODES).find { |j| ip(j) == host }
  end
  nil
end

class Stats
  getter attempted = Set(String).new
  getter confirmed = Set(String).new
  getter nacked = Set(String).new
  getter received = Hash(String, Int32).new(0)
  # When each message was published and confirmed, and through which node
  getter info = Hash(String, String).new
  getter publish_errors = 0
  getter consume_errors = 0
  @lock = Mutex.new

  def attempt(id)
    @lock.synchronize { @attempted << id }
  end

  def confirm(id, ok, detail)
    @lock.synchronize do
      ok ? @confirmed << id : @nacked << id
      @info[id] = detail
    end
  end

  def receive(id)
    @lock.synchronize { @received[id] += 1 }
  end

  def publish_error
    @lock.synchronize { @publish_errors += 1 }
  end

  def consume_error
    @lock.synchronize { @consume_errors += 1 }
  end

  def last_received_count
    @lock.synchronize { @received.size }
  end
end

STATS           = QUEUES.to_h { |q, _| {q, Stats.new} }
STOP_PUBLISHING = Atomic(Bool).new(false)
STOP_ALL        = Atomic(Bool).new(false)

def connect
  connect_with_node[0]
end

def connect_with_node
  i = RNG.rand(NODES)
  {AMQP::Client.new(host: ip(i), port: 5672, user: "guest", password: "guest", heartbeat: 3_u16).connect, i}
end

def ts
  Time.utc.to_s("%H:%M:%S.%L")
end

def declare(ch, queue, type)
  args = AMQP::Client::Arguments.new
  args["x-queue-type"] = type
  ch.queue_declare(queue, durable: true, args: args)
end

# In a method of its own so the confirm callback captures this publish's id,
# a block in the publish loop would see the loop variable's latest value
def publish(ch, queue, id, stats, window, node)
  stats.attempt(id)
  published = ts
  props = AMQP::Client::Properties.new(delivery_mode: 2_u8)
  ch.basic_publish(id, "", queue, props: props) do |ok|
    # false also when the connection closed before the confirm
    stats.confirm(id, ok, "via node#{node + 1}, published #{published}, confirmed #{ts}")
    window.receive?
  end
end

def publisher(pid, queue, type)
  stats = STATS[queue]
  seq = 0
  until STOP_PUBLISHING.get
    begin
      conn, node = connect_with_node
      ch = conn.channel
      ch.confirm_select
      declare(ch, queue, type)
      window = Channel(Nil).new(INFLIGHT)
      until STOP_PUBLISHING.get || conn.closed?
        id = "#{queue}-#{pid}-#{seq += 1}"
        window.send nil
        publish(ch, queue, id, stats, window, node)
      end
      ch.wait_for_confirms rescue nil
      conn.close rescue nil
    rescue
      stats.publish_error
      conn.try &.close rescue nil
      sleep 200.milliseconds
    end
  end
end

def consumer(cid, queue, type)
  stats = STATS[queue]
  # A stream is read from the start by every consumer connection, the
  # deliveries are tracked as a set so re-reads don't matter
  until STOP_ALL.get
    begin
      conn = connect
      ch = conn.channel
      ch.prefetch(500)
      declare(ch, queue, type)
      args = AMQP::Client::Arguments.new
      args["x-stream-offset"] = "first" if type == "stream"
      ch.basic_consume(queue, no_ack: false, args: args) do |msg|
        stats.receive(msg.body_io.to_s)
        msg.ack
      end
      until conn.closed? || ch.closed? || STOP_ALL.get
        sleep 100.milliseconds
      end
      conn.close rescue nil
    rescue
      stats.consume_error
      conn.try &.close rescue nil
      sleep 200.milliseconds
    end
  end
end

def queue_depth(queue) : Int64?
  leader_i = leader || return
  resp = http(leader_i, "GET", "/api/queues/%2f/#{queue}") rescue return
  return unless resp.status_code == 200
  JSON.parse(resp.body)["messages"]?.try(&.as_i64?)
end

class Nemesis
  getter ops = Hash(String, Int32).new(0)
  getter failed = Hash(String, Int32).new(0)

  def pick : String
    total = OPS.sum(&.[1])
    r = RNG.rand(total)
    OPS.each do |op, w|
      return op if r < w
      r -= w
    end
    OPS.first[0]
  end

  def run(deadline)
    while Time.instant < deadline
      sleep INTERVAL
      op = pick
      ok = begin
        perform(op)
      rescue ex
        log "nemesis #{op} raised #{ex.message}"
        false
      end
      @ops[op] += 1
      @failed[op] += 1 unless ok
    end
  end

  private def perform(op) : Bool
    l = leader
    case op
    when "transfer"
      return false unless l
      resp = http(l, "POST", "/api/cluster/transfer-leadership", "{}")
      log "nemesis transfer from node#{l + 1}: #{resp.status_code} #{resp.body}"
      resp.status_code == 202
    when "kill_leader"
      return false unless l
      log "nemesis kill -9 leader node#{l + 1}"
      NODE_LIST[l].kill
      sleep RNG.rand(1.0..4.0).seconds
      NODE_LIST[l].start
      true
    when "pause_leader"
      return false unless l
      d = RNG.rand(1.0..5.0)
      log "nemesis SIGSTOP leader node#{l + 1} for #{d.round(1)}s"
      NODE_LIST[l].signal(Signal::STOP)
      sleep d.seconds
      NODE_LIST[l].signal(Signal::CONT)
      true
    when "kill_follower"
      followers = NODE_LIST.reject { |n| n.i == l }
      n = followers.sample(RNG)
      log "nemesis kill -9 follower node#{n.i + 1}"
      n.kill
      sleep RNG.rand(1.0..4.0).seconds
      n.start
      true
    when "restart_all"
      log "nemesis kill -9 all nodes"
      NODE_LIST.each &.kill
      sleep 1.second
      NODE_LIST.each &.start
      true
    else
      raise "unknown op #{op}"
    end
  end
end

def wait_for_leader(timeout)
  deadline = Time.instant + timeout
  while Time.instant < deadline
    l = leader
    if l
      resp = http(l, "GET", "/api/overview") rescue nil
      return l if resp && resp.status_code == 200
    end
    sleep 500.milliseconds
  end
  nil
end

log "seed #{SEED}, #{NODES} nodes, #{DURATION.total_seconds}s, ops #{OPS}, queues #{QUEUES}"
NODE_LIST.each &.start
unless l = wait_for_leader(60.seconds)
  log "no leader after 60s"
  NODE_LIST.each &.stop
  exit 2
end
log "node#{l + 1} leads"

QUEUES.each do |q, type|
  PUBS.times { |p| spawn(name: "publisher #{q} #{p}") { publisher(p, q, type) } }
  2.times { |c| spawn(name: "consumer #{q} #{c}") { consumer(c, q, type) } } if CONSUME_DURING
end

spawn(name: "progress") do
  until STOP_ALL.get
    sleep 10.seconds
    STATS.each do |q, s|
      log "#{q}: attempted #{s.attempted.size} confirmed #{s.confirmed.size} received #{s.received.size} pub_err #{s.publish_errors} con_err #{s.consume_errors}"
    end
  end
end

nemesis = Nemesis.new
nemesis.run(Time.instant + DURATION)
log "nemesis done: #{nemesis.ops} failed #{nemesis.failed}"

# Heal: every node up, a serving leader
NODE_LIST.each(&.signal(Signal::CONT, expected: false))
NODE_LIST.each &.start
healed = wait_for_leader(60.seconds)
log healed ? "healed, node#{healed + 1} leads" : "!!! no serving leader 60s after healing"
unless CONSUME_DURING
  QUEUES.each do |q, type|
    2.times { |c| spawn(name: "consumer #{q} #{c}") { consumer(c, q, type) } }
  end
end
sleep 5.seconds
STOP_PUBLISHING.set(true)
sleep 3.seconds

# Drain: until every confirmed message is received or nothing new arrives
drain_deadline = Time.instant + 120.seconds
last = -1
stable_since = Time.instant
while Time.instant < drain_deadline
  missing = STATS.sum { |_, s| (s.confirmed - s.received.keys.to_set).size }
  break if missing == 0
  total = STATS.sum { |_, s| s.last_received_count }
  if total != last
    last = total
    stable_since = Time.instant
  elsif Time.instant - stable_since > 30.seconds
    log "nothing received for 30s, #{missing} confirmed messages missing"
    break
  end
  sleep 1.second
end
STOP_ALL.set(true)

# Everything a stream holds now, read from the start by a new consumer
def read_stream(queue) : Set(String)
  ids = Set(String).new
  conn = connect
  ch = conn.channel
  ch.prefetch(1000)
  last = Time.instant
  args = AMQP::Client::Arguments.new
  args["x-stream-offset"] = "first"
  ch.basic_consume(queue, no_ack: false, args: args) do |msg|
    ids << msg.body_io.to_s
    last = Time.instant
    msg.ack
  end
  until Time.instant - last > 5.seconds
    sleep 500.milliseconds
  end
  conn.close
  ids
end

failures = [] of String
QUEUES.each do |q, type|
  next unless type == "stream"
  s = STATS[q]
  final = begin
    read_stream(q)
  rescue ex
    log "reading #{q} failed: #{ex.inspect}"
    nil
  end
  unless final
    failures << "#{q}: couldn't read the stream at the end"
    next
  end
  missing = s.confirmed - final
  vanished = s.received.keys.to_set - final
  log "#{q}: holds #{final.size} at the end, #{missing.size} confirmed missing, #{vanished.size} delivered earlier but gone"
  failures << "#{q}: #{missing.size} confirmed messages not in the stream, e.g. #{missing.first(10).to_a}" unless missing.empty?
  missing.to_a.sort.first(200).each { |id| log "  missing #{id}: #{s.info[id]?}" }
  vanished.to_a.sort.first(200).each { |id| log "  vanished #{id}: #{s.info[id]? || "not confirmed"}" }
end
STATS.each do |q, s|
  received = s.received.keys.to_set
  lost = s.confirmed - received
  unexpected = received - s.attempted
  dups = s.received.count { |_, c| c > 1 }
  indeterminate = s.attempted - s.confirmed - s.nacked
  depth = queue_depth(q)
  log "#{q}: attempted #{s.attempted.size}, confirmed #{s.confirmed.size}, nacked #{s.nacked.size}, " \
      "indeterminate #{indeterminate.size} (#{(indeterminate & received).size} delivered), " \
      "received #{received.size}, duplicates #{dups}, lost #{lost.size}, unexpected #{unexpected.size}, depth #{depth.inspect}"
  failures << "#{q}: #{lost.size} confirmed messages lost, e.g. #{lost.first(10).to_a}" unless lost.empty?
  lost.to_a.sort.each { |id| log "  lost #{id}: #{s.info[id]?}" }
  failures << "#{q}: #{unexpected.size} messages never published, e.g. #{unexpected.first(10).to_a}" unless unexpected.empty?
end
failures << "no serving leader after healing" unless healed
NODE_LIST.each { |n| n.crashes.each { |c| failures << c } }

NODE_LIST.each &.stop
NODE_LIST.each do |n|
  File.each_line(n.log_path) do |line|
    if line =~ /Unhandled exception|Invalid memory access|FATAL|segmentation fault/i
      failures << "node#{n.i + 1} log: #{line[0, 300]}"
    end
  end
end

if failures.empty?
  log "PASS"
else
  log "FAIL"
  failures.uniq.first(100).each { |f| log "  #{f}" }
  exit 1
end
