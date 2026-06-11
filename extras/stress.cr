# Stress driver for LavinMQ.
#
# Spawns many concurrent AMQP and HTTP workers against a running server,
# deliberately interleaving traffic with topology churn (bind/unbind,
# declare/delete, connection kills, vhost churn, dead-lettering, TTLs) to
# surface multi-threading bugs.
#
# Usage:
#   crystal run --release extras/stress.cr -- --duration 60 --concurrency 8
#
# The server must be running separately, e.g.:
#   bin/lavinmq --data-dir ./tmp/stress --debug

require "amqp-client"
require "http/client"
require "option_parser"
require "uri"
require "json"
require "base64"
require "log"

count = Fiber::ExecutionContext.default_workers_count
Fiber::ExecutionContext.default.resize(count)

module Stress
  alias Arguments = AMQ::Protocol::Table
  alias Properties = AMQ::Protocol::Properties

  class Config
    property host = "localhost"
    property amqp_port = 5672
    property http_port = 15672
    property user = "guest"
    property password = "guest"
    property vhost = "stress"
    property duration = 120
    property concurrency = 4

    def amqp_url : String
      "amqp://#{@user}:#{@password}@#{@host}:#{@amqp_port}/#{URI.encode_path_segment(@vhost)}"
    end

    def http_base : String
      "http://#{@host}:#{@http_port}"
    end
  end

  class Stats
    {% for name in %w(
                     published acked nacked rejected dead_lettered get_ok get_empty
                     confirmed confirm_nacked confirm_timeouts confirm_disconnects batch_confirm_cycles
                     bindings_added bindings_removed
                     exchanges_added exchanges_removed
                     queues_added queues_removed
                     vhosts_added vhosts_removed
                     policies_added policies_removed
                     connections_opened connections_closed
                     channels_opened channels_closed
                     http_publishes http_lists http_conn_kills
                     stream_published stream_acked stream_consumer_cycles
                     stream_varied_consumer_cycles stream_filtered_consumer_cycles
                     stream_filtered_received
                     fed_ex_published fed_ex_received fed_q_published fed_q_received
                     fed_links_added fed_links_removed fed_churn_cycles
                     queues_paused queues_resumed
                     purges errors
                   ) %}
      @{{name.id}} = Atomic(Int64).new(0_i64)
      def {{name.id}} : Int64
        @{{name.id}}.get
      end
      def incr_{{name.id}}(by = 1_i64) : Nil
        @{{name.id}}.add(by.to_i64)
      end
    {% end %}
  end

  CFG   = Config.new
  STOP  = Atomic(Bool).new(false)
  STATS = Stats.new

  # Topology constants
  EX_TOPIC   = "stress.topic"
  EX_HEADERS = "stress.headers"
  EX_DIRECT  = "stress.direct"
  EX_FANOUT  = "stress.fanout"
  EX_DLX     = "stress.dlx"

  Q_MAIN       = "stress.q"
  Q_DLQ        = "stress.dlq"
  Q_POLL       = "stress.pollq"
  Q_STREAM     = "stress.stream"
  NOISE_QUEUES = 16

  # Stream tuning. The segment file is 8 MiB by default. We want two things
  # to happen concurrently:
  #
  #   1. Segments roll over and `drop_overflow` runs (which also calls
  #      `cleanup_consumer_offsets` on every drop). Tight `x-max-length-bytes`
  #      forces this — keep just a couple of segments live at a time.
  #
  #   2. The `consumer_offsets` file (capacity = segment_size = 8 MiB) fills
  #      between `drop_overflow` compactions, hitting the `file_full?` branch
  #      inside `store_consumer_offset`. To force this we want many acks per
  #      KiB written: small payloads. Each ack appends ~(1 + tag.bytesize + 8)
  #      bytes to the offsets file regardless of whether the tag is new.
  STREAM_PAYLOAD_SIZE     = 512
  STREAM_MAX_LENGTH_BYTES = 8 * 1024 * 1024

  # Filter values stamped on a fraction of stream messages via the
  # `x-stream-filter-value` header. The set is intentionally small so
  # filtered consumers actually match some messages.
  STREAM_FILTER_VALUES = %w(v1 v2 v3 v4 v5)

  TOPIC_KEYS   = %w(a.b.c a.b.d a.x.y x.b.c k.l.m foo.bar foo.baz hot.cold ping.pong red.green.blue)
  HEADER_TYPES = %w(a b c d)

  # Federation topology. Two extra vhosts ("a" and "b") each carrying a
  # topic exchange and two queues — one bound to the exchange for exchange
  # federation, one stand-alone for queue federation. Each vhost has an
  # upstream pointing at the other so the wiring is bidirectional. Policies
  # attach the upstream to the local exchange/queue. max-hops=1 on the
  # exchange upstream prevents republish loops.
  FED_VHOSTS = {:a => "stress-fed-a", :b => "stress-fed-b"}
  FED_EX     = "fed.ex"
  FED_Q      = "fed.q"
  FED_QF     = "fed.qf"

  record FedUpstream, downstream : Symbol, upstream : Symbol, name : String, kind : Symbol

  FED_UPSTREAMS = [
    FedUpstream.new(:a, :b, "from-b-ex", :exchange),
    FedUpstream.new(:a, :b, "from-b-q", :queue),
    FedUpstream.new(:b, :a, "from-a-ex", :exchange),
    FedUpstream.new(:b, :a, "from-a-q", :queue),
  ]

  def self.log_info(msg)
    STDERR.puts "[stress] #{Time.utc.to_s("%H:%M:%S")} #{msg}"
  end

  def self.parse_args
    OptionParser.parse do |p|
      p.banner = "Usage: stress [options]"
      p.on("--host=HOST", "Server host (default localhost)") { |v| CFG.host = v }
      p.on("--amqp-port=PORT", "AMQP port (default 5672)") { |v| CFG.amqp_port = v.to_i }
      p.on("--http-port=PORT", "HTTP port (default 15672)") { |v| CFG.http_port = v.to_i }
      p.on("--user=USER", "Username (default guest)") { |v| CFG.user = v }
      p.on("--password=PASS", "Password (default guest)") { |v| CFG.password = v }
      p.on("--vhost=VHOST", "Vhost (default stress)") { |v| CFG.vhost = v }
      p.on("--duration=SEC", "Duration in seconds (default 60)") { |v| CFG.duration = v.to_i }
      p.on("--concurrency=N", "Fibers per worker class (default 4)") { |v| CFG.concurrency = v.to_i }
      p.on("-h", "--help", "Show help") { puts p; exit 0 }
    end
  end

  # -------- HTTP helpers --------

  def self.http_request(method : String, path : String, body : String? = nil, read_timeout : Time::Span = 10.seconds) : {Int32, String}
    headers = HTTP::Headers{
      "Authorization" => "Basic #{Base64.strict_encode("#{CFG.user}:#{CFG.password}")}",
      "Content-Type"  => "application/json",
    }
    uri = URI.parse("#{CFG.http_base}#{path}")
    client = HTTP::Client.new(uri)
    client.connect_timeout = 5.seconds
    client.read_timeout = read_timeout
    resp = client.exec(method, uri.request_target, headers, body)
    {resp.status_code, resp.body.to_s}
  ensure
    client.try &.close
  end

  def self.http_put(path : String, body : String? = nil) : Int32
    http_request("PUT", path, body)[0]
  end

  def self.http_delete(path : String) : Int32
    http_request("DELETE", path)[0]
  end

  def self.http_post(path : String, body : String? = nil) : Int32
    http_request("POST", path, body)[0]
  end

  def self.http_get(path : String) : {Int32, String}
    http_request("GET", path)
  end

  def self.vhost_path(vhost : String = CFG.vhost) : String
    URI.encode_path_segment(vhost)
  end

  def self.amqp_url_for(vhost : String) : String
    "amqp://#{CFG.user}:#{CFG.password}@#{CFG.host}:#{CFG.amqp_port}/#{URI.encode_path_segment(vhost)}"
  end

  # -------- Topology setup --------

  def self.setup_topology
    # Create stress vhost
    case code = http_put("/api/vhosts/#{vhost_path}")
    when 201, 204
      log_info "Created vhost #{CFG.vhost}"
    else
      log_info "Vhost #{CFG.vhost} create returned #{code} (probably already exists)"
    end

    # Give guest user permissions on the vhost
    perm_body = %({"configure":".*","write":".*","read":".*"})
    http_put("/api/permissions/#{vhost_path}/#{URI.encode_path_segment(CFG.user)}", perm_body)

    # Connect via AMQP and declare the fixed topology
    AMQP::Client.start(CFG.amqp_url) do |conn|
      ch = conn.channel

      ch.exchange_declare EX_TOPIC, "topic", durable: true
      ch.exchange_declare EX_HEADERS, "headers", durable: true
      ch.exchange_declare EX_DIRECT, "direct", durable: true
      ch.exchange_declare EX_FANOUT, "fanout", durable: true
      ch.exchange_declare EX_DLX, "fanout", durable: true

      # Main queue with DLX
      ch.queue_declare Q_MAIN, durable: true, args: Arguments.new({
        "x-dead-letter-exchange" => EX_DLX.as(AMQ::Protocol::Field),
      })
      ch.queue_bind Q_MAIN, EX_TOPIC, "#"
      ch.queue_bind Q_MAIN, EX_DIRECT, "main"
      ch.queue_bind Q_MAIN, EX_FANOUT, ""
      ch.queue_bind Q_MAIN, EX_HEADERS, "", args: Arguments.new({
        "x-match" => "any".as(AMQ::Protocol::Field),
        "type"    => "a".as(AMQ::Protocol::Field),
      })

      # DLQ
      ch.queue_declare Q_DLQ, durable: true
      ch.queue_bind Q_DLQ, EX_DLX, ""

      # Polling queue (basic_get only)
      ch.queue_declare Q_POLL, durable: true
      ch.queue_bind Q_POLL, EX_TOPIC, "k.#"

      # Stream queue with tight overflow so segments roll and drop
      # continuously while producers run.
      ch.queue_declare Q_STREAM, durable: true, args: Arguments.new({
        "x-queue-type"       => "stream".as(AMQ::Protocol::Field),
        "x-max-length-bytes" => STREAM_MAX_LENGTH_BYTES.to_i64.as(AMQ::Protocol::Field),
      })

      # Noise queues for binding churn
      NOISE_QUEUES.times do |i|
        ch.queue_declare "stress.noise.#{i}", durable: false, auto_delete: false
      end
    end
    log_info "Topology ready"

    setup_federation_topology
  end

  def self.setup_federation_topology
    FED_VHOSTS.each_value do |vh|
      case code = http_put("/api/vhosts/#{vhost_path(vh)}")
      when 201, 204
        log_info "Created vhost #{vh}"
      else
        log_info "Vhost #{vh} create returned #{code} (probably already exists)"
      end
      perm_body = %({"configure":".*","write":".*","read":".*"})
      http_put("/api/permissions/#{vhost_path(vh)}/#{URI.encode_path_segment(CFG.user)}", perm_body)

      AMQP::Client.start(amqp_url_for(vh)) do |conn|
        ch = conn.channel
        ch.exchange_declare FED_EX, "topic", durable: true
        ch.queue_declare FED_Q, durable: true
        ch.queue_bind FED_Q, FED_EX, "#"
        ch.queue_declare FED_QF, durable: true
      end
    end

    FED_UPSTREAMS.each do |u|
      put_fed_upstream(FED_VHOSTS[u.downstream], FED_VHOSTS[u.upstream], u.name, u.kind)
      STATS.incr_fed_links_added
    end

    # One policy per (vhost, kind). Matches the local resource and binds it
    # to the upstream parameter named above.
    FED_VHOSTS.each do |key, vh|
      upstream_ex_name = key == :a ? "from-b-ex" : "from-a-ex"
      upstream_q_name = key == :a ? "from-b-q" : "from-a-q"
      put_fed_policy(vh, "fed-ex-policy", "^#{Regex.escape(FED_EX)}$", "exchanges", upstream_ex_name)
      put_fed_policy(vh, "fed-q-policy", "^#{Regex.escape(FED_QF)}$", "queues", upstream_q_name)
    end

    log_info "Federation ready"
  end

  def self.put_fed_upstream(downstream_vhost : String, upstream_vhost : String, name : String, kind : Symbol) : Int32
    body = JSON.build do |json|
      json.object do
        json.field "value" do
          json.object do
            json.field "uri", amqp_url_for(upstream_vhost)
            json.field "ack-mode", "on-confirm"
            json.field "reconnect-delay", 1
            json.field "prefetch-count", 100
            case kind
            when :exchange
              json.field "exchange", FED_EX
              json.field "max-hops", 1
            when :queue
              json.field "queue", FED_QF
            end
          end
        end
      end
    end
    path = "/api/parameters/federation-upstream/#{vhost_path(downstream_vhost)}/#{URI.encode_path_segment(name)}"
    http_put(path, body)
  end

  def self.put_fed_policy(vhost : String, policy_name : String, pattern : String, apply_to : String, upstream_name : String) : Int32
    body = JSON.build do |json|
      json.object do
        json.field "pattern", pattern
        json.field "apply-to", apply_to
        json.field "priority", 20
        json.field "definition" do
          json.object do
            json.field "federation-upstream", upstream_name
          end
        end
      end
    end
    http_put("/api/policies/#{vhost_path(vhost)}/#{URI.encode_path_segment(policy_name)}", body)
  end

  # -------- Worker base helpers --------

  def self.run_until_stop(name : String, sleep_on_error : Time::Span = 50.milliseconds, &)
    until STOP.get
      begin
        yield
      rescue ex
        STATS.incr_errors
        Log.debug { "[#{name}] #{ex.class}: #{ex.message}" }
        sleep sleep_on_error unless STOP.get
      end
    end
  end

  def self.with_amqp(name : String, url : String = CFG.amqp_url, &)
    until STOP.get
      begin
        AMQP::Client.start(url) do |conn|
          STATS.incr_connections_opened
          yield conn
          STATS.incr_connections_closed
        end
      rescue ex
        STATS.incr_errors
        Log.debug { "[#{name}] #{ex.class}: #{ex.message}" }
        sleep 100.milliseconds unless STOP.get
      end
    end
  end

  # -------- Producers --------

  private def self.publish_for_kind(ch, exchange : String, kind : Symbol, body : String, args : Properties)
    case kind
    when :topic
      ch.basic_publish(body, exchange, TOPIC_KEYS.sample, props: args)
    when :headers
      h = Arguments.new({
        "type" => HEADER_TYPES.sample.as(AMQ::Protocol::Field),
        "n"    => Random.rand(10).to_i64.as(AMQ::Protocol::Field),
      })
      props = Properties.new(delivery_mode: args.delivery_mode, expiration: args.expiration, headers: h)
      ch.basic_publish(body, exchange, "", props: props)
    when :direct
      ch.basic_publish(body, exchange, "main", props: args)
    when :fanout
      ch.basic_publish(body, exchange, "", props: args)
    end
  end

  # mode: :fast | :slow | :bursty
  def self.producer(id : Int32, exchange : String, mode : Symbol, kind : Symbol)
    with_amqp("prod-#{kind}-#{mode}-#{id}") do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        seq &+= 1
        body = "msg-#{kind}-#{mode}-#{id}-#{seq}"
        args = Properties.new(delivery_mode: 1_u8)
        # 10% messages carry per-message TTL for DLX path
        if Random.rand(10) == 0
          args = Properties.new(delivery_mode: 1_u8, expiration: Random.rand(500).to_s)
        end
        publish_for_kind(ch, exchange, kind, body, args)
        STATS.incr_published

        case mode
        when :slow
          sleep Random.rand(5..50).milliseconds
        when :bursty
          if seq % 50 == 0
            sleep Random.rand(100..500).milliseconds
          end
        end
      end
    end
  end

  # -------- Confirm-mode producers --------
  #
  # The block form of `basic_publish` puts the channel in confirm mode and
  # invokes the block when the broker's confirm arrives. We use a buffered
  # `Channel(Bool)` plus Crystal's `select`/`timeout` to bound the wait —
  # `amqp-client`'s `basic_publish_confirm` sleeps unbounded, which is fine
  # for normal use but defeats the point in a stress driver where we want
  # to *see* timeouts. On channel/connection close, pending callbacks fire
  # with `false`, which we count as nacks.

  CONFIRM_TIMEOUT       = 5.seconds
  BATCH_CONFIRM_SIZE    = 100
  BATCH_CONFIRM_TIMEOUT = 10.seconds

  private def self.confirm_routing(kind : Symbol) : {String, Properties}
    rkey = case kind
           when :topic  then TOPIC_KEYS.sample
           when :direct then "main"
           else              ""
           end
    props = if kind == :headers
              Properties.new(delivery_mode: 1_u8, headers: Arguments.new({
                "type" => HEADER_TYPES.sample.as(AMQ::Protocol::Field),
              }))
            else
              Properties.new(delivery_mode: 1_u8)
            end
    {rkey, props}
  end

  # Synchronous confirmer: publish one, wait up to CONFIRM_TIMEOUT for the
  # confirm, count ack/nack/timeout. Runs against the same exchanges as the
  # fire-and-forget producers so confirm-mode and non-confirm-mode channels
  # race on Q_MAIN.
  def self.confirm_producer(id : Int32, exchange : String, kind : Symbol)
    with_amqp("conf-#{kind}-#{id}") do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        seq &+= 1
        rkey, props = confirm_routing(kind)
        body = "conf-#{kind}-#{id}-#{seq}"
        result = ::Channel(Bool).new(1)
        ch.basic_publish(body, exchange, rkey, props: props) do |ok|
          result.send(ok) rescue nil
        end
        STATS.incr_published
        select
        when ok = result.receive
          if ok
            STATS.incr_confirmed
          elsif ch.closed? || conn.closed?
            # cleanup() fires every pending callback with false on
            # channel/connection teardown — not a real nack
            STATS.incr_confirm_disconnects
          else
            STATS.incr_confirm_nacked
            Log.warn { "[conf-#{kind}-#{id}] nack seq=#{seq}" }
          end
        when timeout(CONFIRM_TIMEOUT)
          STATS.incr_confirm_timeouts
          Log.warn { "[conf-#{kind}-#{id}] confirm timeout seq=#{seq}" }
        end
      end
    end
  end

  # Batched confirmer: publish BATCH_CONFIRM_SIZE without waiting between
  # them, then drain the confirm callbacks with a total deadline. Each
  # arriving confirm is counted individually; messages whose confirm never
  # shows up before the deadline are counted as timeouts.
  def self.batch_confirm_producer(id : Int32, exchange : String, kind : Symbol)
    with_amqp("bconf-#{kind}-#{id}") do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        ack_chan = ::Channel(Bool).new(BATCH_CONFIRM_SIZE)
        published = 0
        BATCH_CONFIRM_SIZE.times do
          break if STOP.get || ch.closed?
          seq &+= 1
          rkey, props = confirm_routing(kind)
          body = "bconf-#{kind}-#{id}-#{seq}"
          ch.basic_publish(body, exchange, rkey, props: props) do |ok|
            ack_chan.send(ok) rescue nil
          end
          STATS.incr_published
          published += 1
        end
        next if published == 0
        received = 0
        deadline = Time.instant + BATCH_CONFIRM_TIMEOUT
        while received < published && !STOP.get
          remaining = deadline - Time.instant
          break if remaining <= Time::Span.zero
          select
          when ok = ack_chan.receive
            received += 1
            if ok
              STATS.incr_confirmed
            elsif ch.closed? || conn.closed?
              STATS.incr_confirm_disconnects
            else
              STATS.incr_confirm_nacked
            end
          when timeout(remaining)
            break
          end
        end
        missing = published - received
        if missing > 0
          # Don't warn if we already know the connection died — that explains
          # the missing confirms (cleanup() drains the pending deque but our
          # already-timed-out `select` no longer sees them).
          if ch.closed? || conn.closed?
            STATS.incr_confirm_disconnects(by: missing.to_i64)
          else
            STATS.incr_confirm_timeouts(by: missing.to_i64)
            Log.warn { "[bconf-#{kind}-#{id}] #{missing}/#{published} confirms missing after #{BATCH_CONFIRM_TIMEOUT}" }
          end
        end
        STATS.incr_batch_confirm_cycles
      end
    end
  end

  # -------- Consumers --------

  # consume_mode: :fast | :slow | :bursty
  def self.consumer(id : Int32, queue : String, consume_mode : Symbol, reject_rate : Float64 = 0.0)
    with_amqp("cons-#{consume_mode}-#{id}") do |conn|
      ch = conn.channel
      ch.prefetch(count: 20)
      seq = Atomic(Int64).new(0_i64)
      tag = ch.basic_consume(queue, tag: "cons-#{consume_mode}-#{id}-#{Random.rand(1_000_000)}", no_ack: false, block: false) do |msg|
        n = seq.add(1_i64) + 1
        begin
          if reject_rate > 0 && Random.rand < reject_rate
            msg.reject(requeue: false)
            STATS.incr_rejected
            STATS.incr_dead_lettered
          else
            case consume_mode
            when :slow
              sleep Random.rand(5..50).milliseconds
            when :bursty
              sleep Random.rand(100..500).milliseconds if n % 50 == 0
            end
            msg.ack
            STATS.incr_acked
          end
        rescue ex
          # Connection/channel killed under us mid-ack. The with_amqp loop will reconnect.
          STATS.incr_errors
        end
      end
      until STOP.get || ch.closed? || conn.closed?
        sleep 200.milliseconds
      end
      ch.basic_cancel(tag) rescue nil
      ch.close rescue nil
    end
  end

  # -------- basic_get pollers --------

  def self.poller(id : Int32, queue : String)
    with_amqp("poll-#{id}") do |conn|
      ch = conn.channel
      while !STOP.get && !ch.closed?
        if msg = ch.basic_get(queue, no_ack: false)
          STATS.incr_get_ok
          if Random.rand < 0.1
            msg.reject(requeue: true)
          else
            msg.ack
          end
        else
          STATS.incr_get_empty
          sleep 20.milliseconds
        end
      end
    end
  end

  # -------- Bind churn --------

  def self.topic_bind_churn(id : Int32)
    with_amqp("bind-topic-#{id}") do |conn|
      ch = conn.channel
      while !STOP.get && !ch.closed?
        q = "stress.noise.#{Random.rand(NOISE_QUEUES)}"
        rkey = TOPIC_KEYS.sample
        begin
          ch.queue_bind(q, EX_TOPIC, rkey)
          STATS.incr_bindings_added
        rescue
          STATS.incr_errors
        end
        sleep Random.rand(1..10).milliseconds
        begin
          ch.queue_unbind(q, EX_TOPIC, rkey)
          STATS.incr_bindings_removed
        rescue
          STATS.incr_errors
        end
      end
    end
  end

  def self.headers_bind_churn(id : Int32)
    with_amqp("bind-headers-#{id}") do |conn|
      ch = conn.channel
      while !STOP.get && !ch.closed?
        q = "stress.noise.#{Random.rand(NOISE_QUEUES)}"
        match = Random.rand(2) == 0 ? "any" : "all"
        args = Arguments.new({
          "x-match" => match.as(AMQ::Protocol::Field),
          "type"    => HEADER_TYPES.sample.as(AMQ::Protocol::Field),
          "n"       => Random.rand(10).to_i64.as(AMQ::Protocol::Field),
        })
        begin
          ch.queue_bind(q, EX_HEADERS, "", args: args)
          STATS.incr_bindings_added
        rescue
          STATS.incr_errors
        end
        sleep Random.rand(1..10).milliseconds
        begin
          ch.queue_unbind(q, EX_HEADERS, "", args: args)
          STATS.incr_bindings_removed
        rescue
          STATS.incr_errors
        end
      end
    end
  end

  def self.direct_fanout_bind_churn(id : Int32)
    with_amqp("bind-df-#{id}") do |conn|
      ch = conn.channel
      while !STOP.get && !ch.closed?
        q = "stress.noise.#{Random.rand(NOISE_QUEUES)}"
        if Random.rand(2) == 0
          rkey = "rk-#{Random.rand(8)}"
          begin
            ch.queue_bind(q, EX_DIRECT, rkey)
            STATS.incr_bindings_added
            sleep Random.rand(1..10).milliseconds
            ch.queue_unbind(q, EX_DIRECT, rkey)
            STATS.incr_bindings_removed
          rescue
            STATS.incr_errors
          end
        else
          begin
            ch.queue_bind(q, EX_FANOUT, "")
            STATS.incr_bindings_added
            sleep Random.rand(1..10).milliseconds
            ch.queue_unbind(q, EX_FANOUT, "")
            STATS.incr_bindings_removed
          rescue
            STATS.incr_errors
          end
        end
      end
    end
  end

  # -------- Exchange churn --------

  EX_TYPES = %w(direct fanout topic headers)

  def self.exchange_churn(id : Int32)
    with_amqp("ex-churn-#{id}") do |conn|
      ch = conn.channel
      n = 0_u64
      while !STOP.get && !ch.closed?
        n &+= 1
        type = EX_TYPES.sample
        name = "stress.x.#{type}.#{id}.#{n}"
        begin
          ch.exchange_declare(name, type, durable: false, auto_delete: false)
          STATS.incr_exchanges_added
          # Attach a noise queue briefly
          q = "stress.noise.#{Random.rand(NOISE_QUEUES)}"
          rkey = type == "topic" ? "#" : (type == "headers" ? "" : "k")
          if type == "headers"
            args = Arguments.new({"x-match" => "any".as(AMQ::Protocol::Field), "type" => "a".as(AMQ::Protocol::Field)})
            ch.queue_bind(q, name, "", args: args) rescue nil
          else
            ch.queue_bind(q, name, rkey) rescue nil
          end
          sleep Random.rand(5..30).milliseconds
          ch.exchange_delete(name)
          STATS.incr_exchanges_removed
        rescue
          STATS.incr_errors
        end
        sleep Random.rand(1..20).milliseconds
      end
    end
  end

  # -------- Queue churn --------

  def self.queue_churn(id : Int32)
    with_amqp("q-churn-#{id}") do |conn|
      ch = conn.channel
      n = 0_u64
      while !STOP.get && !ch.closed?
        n &+= 1
        name = "stress.tmpq.#{id}.#{n}"
        args = Arguments.new
        # Pick a random arg mix
        case Random.rand(8)
        when 0
          args["x-message-ttl"] = Random.rand(0..500).to_i64.as(AMQ::Protocol::Field)
          args["x-dead-letter-exchange"] = EX_DLX.as(AMQ::Protocol::Field)
        when 1
          args["x-expires"] = Random.rand(200..2000).to_i64.as(AMQ::Protocol::Field)
        when 2
          args["x-max-length"] = Random.rand(1..50).to_i64.as(AMQ::Protocol::Field)
          args["x-dead-letter-exchange"] = EX_DLX.as(AMQ::Protocol::Field)
        when 3
          args["x-max-length-bytes"] = Random.rand(1024..16_384).to_i64.as(AMQ::Protocol::Field)
          args["x-overflow"] = "drop-head".as(AMQ::Protocol::Field)
        when 4
          args["x-max-length"] = Random.rand(1..20).to_i64.as(AMQ::Protocol::Field)
          args["x-overflow"] = "reject-publish".as(AMQ::Protocol::Field)
        when 5
          # Stream queue (small fraction)
          args["x-queue-type"] = "stream".as(AMQ::Protocol::Field)
        when 6
          args["x-dead-letter-exchange"] = EX_DLX.as(AMQ::Protocol::Field)
          args["x-dead-letter-routing-key"] = "via-dlx".as(AMQ::Protocol::Field)
        else
          # plain queue
        end
        durable = Random.rand(2) == 0
        auto_delete = Random.rand(3) == 0
        begin
          ch.queue_declare(name, durable: durable, auto_delete: auto_delete, args: args)
          STATS.incr_queues_added
          # Sometimes bind to live exchange so unbind races publish
          if Random.rand(2) == 0
            rkey = TOPIC_KEYS.sample
            ch.queue_bind(name, EX_TOPIC, rkey) rescue nil
          end
          sleep Random.rand(10..100).milliseconds
          ch.queue_delete(name) rescue nil
          STATS.incr_queues_removed
        rescue
          STATS.incr_errors
        end
      end
    end
  end

  # -------- Channel / connection churn --------

  def self.channel_churn(id : Int32)
    with_amqp("ch-churn-#{id}") do |conn|
      while !STOP.get
        begin
          ch = conn.channel
          STATS.incr_channels_opened
          sleep Random.rand(1..30).milliseconds
          ch.close rescue nil
          STATS.incr_channels_closed
        rescue
          STATS.incr_errors
          break
        end
      end
    end
  end

  def self.connection_churn(id : Int32)
    while !STOP.get
      begin
        AMQP::Client.start(CFG.amqp_url) do |conn|
          STATS.incr_connections_opened
          ch = conn.channel
          # Do a tiny bit of work to make sure the connection is real
          ch.queue_declare("stress.noise.#{Random.rand(NOISE_QUEUES)}", durable: false, passive: true) rescue nil
          sleep Random.rand(10..200).milliseconds
        end
        STATS.incr_connections_closed
      rescue
        STATS.incr_errors
      end
      sleep Random.rand(10..100).milliseconds unless STOP.get
    end
  end

  # -------- Purge churn --------

  # Purge any of the actively-trafficked queues. Picking among the named
  # queues plus the noise pool means every purge competes with live
  # publishes/consumes/binds against that target. Purging Q_STREAM in
  # particular races segment rollover and consumer-offset cleanup.
  def self.purge_churn(id : Int32)
    with_amqp("purge-#{id}") do |conn|
      ch = conn.channel
      while !STOP.get && !ch.closed?
        target = case Random.rand(6)
                 when 0 then Q_MAIN
                 when 1 then Q_DLQ
                 when 2 then Q_POLL
                 when 3 then Q_STREAM
                 else        "stress.noise.#{Random.rand(NOISE_QUEUES)}"
                 end
        begin
          ch.queue_purge(target)
          STATS.incr_purges
        rescue
          STATS.incr_errors
        end
        sleep Random.rand(100..600).milliseconds
      end
    end
  end

  # -------- Stream workers --------
  #
  # Goal: keep segments rolling on `stress.stream` so the periodic
  # `unmap_and_remove_segments_loop` and every `open_new_segment` triggers
  # `drop_overflow` → `cleanup_consumer_offsets`. The consumer churner
  # additionally seeds many distinct tracked tags so the offsets file fills
  # and the cleanup actually has entries to prune.

  STREAM_PAYLOAD = "s" * STREAM_PAYLOAD_SIZE

  # Long tag prefix — the offsets file grows by `1 + tag.bytesize + 8` bytes
  # per ack, so longer tags fill it faster. Anything goes for the suffix
  # since tag uniqueness only matters for the churner.
  STREAM_TAG_PREFIX = "stress-stream-consumer-tag-for-offset-tracking-and-cleanup-pressure-"

  def self.stream_producer(id : Int32, mode : Symbol)
    with_amqp("strp-#{mode}-#{id}") do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        seq &+= 1
        # ~50% of messages carry an x-stream-filter-value so filtered
        # consumers have something to match against. Plain consumers
        # ignore the header.
        props = if Random.rand(2) == 0
                  Properties.new(delivery_mode: 1_u8, headers: Arguments.new({
                    "x-stream-filter-value" => STREAM_FILTER_VALUES.sample.as(AMQ::Protocol::Field),
                  }))
                else
                  Properties.new(delivery_mode: 1_u8)
                end
        ch.basic_publish(STREAM_PAYLOAD, "", Q_STREAM, props: props)
        STATS.incr_stream_published
        # Throttle to keep the stream from crowding out the rest of the
        # stress workload. Even "fast" yields every 50 msgs.
        case mode
        when :fast
          Fiber.yield if seq % 50 == 0
        when :slow
          sleep Random.rand(5..20).milliseconds
        when :bursty
          sleep Random.rand(50..200).milliseconds if seq % 200 == 0
        end
      end
    end
  end

  # Long-lived stream consumer with automatic offset tracking enabled.
  # Each `ack` writes its current offset to the consumer_offsets file.
  # Starts from "first" so it races against drop_overflow: when its segment
  # is dropped, the consumer is fast-forwarded by `find_offset_in_segments`,
  # which exercises the cleanup path that prunes offset entries pointing at
  # dropped segments.
  def self.stream_consumer(id : Int32)
    with_amqp("strc-#{id}") do |conn|
      ch = conn.channel
      ch.prefetch(count: 200)
      tag = "#{STREAM_TAG_PREFIX}cons-#{id}-#{Random.rand(1_000_000_000)}"
      args = Arguments.new({
        "x-stream-offset"                    => "first".as(AMQ::Protocol::Field),
        "x-stream-automatic-offset-tracking" => true.as(AMQ::Protocol::Field),
      })
      ch.basic_consume(Q_STREAM, tag: tag, no_ack: false, block: false, args: args) do |msg|
        begin
          msg.ack
          STATS.incr_stream_acked
        rescue
          STATS.incr_errors
        end
      end
      until STOP.get || ch.closed? || conn.closed?
        sleep 200.milliseconds
      end
    end
  end

  # Short-lived stream consumer cycles. Each iteration uses a fresh,
  # never-reused tag → another entry in consumer_offsets. Eventually the
  # offsets file approaches its capacity and `consumer_offset_file_full?`
  # returns true, forcing `cleanup_consumer_offsets`. Combined with segment
  # rollover this exercises both cleanup paths.
  def self.stream_consumer_churn(id : Int32)
    cycle = 0_u64
    until STOP.get
      cycle &+= 1
      tag = "#{STREAM_TAG_PREFIX}churn-#{id}-#{cycle}-#{Random.rand(1_000_000_000)}"
      begin
        AMQP::Client.start(CFG.amqp_url) do |conn|
          STATS.incr_connections_opened
          ch = conn.channel
          ch.prefetch(count: 20)
          args = Arguments.new({
            "x-stream-offset"                    => "first".as(AMQ::Protocol::Field),
            "x-stream-automatic-offset-tracking" => true.as(AMQ::Protocol::Field),
          })
          got = Atomic(Int32).new(0)
          ch.basic_consume(Q_STREAM, tag: tag, no_ack: false, block: false, args: args) do |msg|
            begin
              msg.ack
              got.add(1)
              STATS.incr_stream_acked
            rescue
              STATS.incr_errors
            end
          end
          # Ack a chunk of messages so many offset entries accumulate for
          # this unique tag, then disconnect — leaving them all behind in
          # consumer_offsets. Bigger target = more pressure on the file.
          target = 100 + Random.rand(400)
          deadline = Time.instant + 3.seconds
          until STOP.get || got.get >= target || Time.instant >= deadline
            sleep 20.milliseconds
          end
          ch.basic_cancel(tag) rescue nil
          STATS.incr_stream_consumer_cycles
          STATS.incr_connections_closed
        end
      rescue
        STATS.incr_errors
      end
      sleep Random.rand(20..100).milliseconds unless STOP.get
    end
  end

  # Pick a randomized `x-stream-offset` value plus a random
  # `x-stream-automatic-offset-tracking` setting. Exercises every branch
  # of `StreamMessageStore#find_offset`: named ("first"/"last"/"next"),
  # numeric (in-range, negative, past-end), and timestamp.
  private def self.random_stream_offset_args : Arguments
    args = Arguments.new
    case Random.rand(8)
    when 0
      args["x-stream-offset"] = "first".as(AMQ::Protocol::Field)
    when 1
      args["x-stream-offset"] = "last".as(AMQ::Protocol::Field)
    when 2
      args["x-stream-offset"] = "next".as(AMQ::Protocol::Field)
    when 3
      # In-range numeric offset
      args["x-stream-offset"] = Random.rand(0_i64..10_000_i64).as(AMQ::Protocol::Field)
    when 4
      # Negative offset — server falls back to first available segment
      args["x-stream-offset"] = (-Random.rand(1_i64..1_000_i64)).as(AMQ::Protocol::Field)
    when 5
      # Offset far past the tail — maps to "next"-like behavior
      args["x-stream-offset"] = (1_000_000_000_i64 + Random.rand(0_i64..1_000_000_i64)).as(AMQ::Protocol::Field)
    when 6
      # Timestamp in the recent past
      ts = Time.utc - Random.rand(0..120).seconds
      args["x-stream-offset"] = ts.as(AMQ::Protocol::Field)
    else
      # Older timestamp before the stream existed
      ts = Time.utc - Random.rand(300..3600).seconds
      args["x-stream-offset"] = ts.as(AMQ::Protocol::Field)
    end
    # Mix in automatic offset tracking on/off, sometimes as the string
    # form "true"/"false" which the server also accepts.
    case Random.rand(4)
    when 0 then args["x-stream-automatic-offset-tracking"] = true.as(AMQ::Protocol::Field)
    when 1 then args["x-stream-automatic-offset-tracking"] = false.as(AMQ::Protocol::Field)
    when 2 then args["x-stream-automatic-offset-tracking"] = "true".as(AMQ::Protocol::Field)
    else # omitted — server default applies
    end
    args
  end

  # Short-lived consumers that exercise every offset variant on the
  # stream queue. Each iteration uses a fresh tag and a fresh
  # offset/tracking combo, then drains for a brief window before
  # disconnecting.
  def self.stream_varied_offset_consumer(id : Int32)
    cycle = 0_u64
    until STOP.get
      cycle &+= 1
      tag = "#{STREAM_TAG_PREFIX}varied-#{id}-#{cycle}-#{Random.rand(1_000_000_000)}"
      args = random_stream_offset_args
      begin
        AMQP::Client.start(CFG.amqp_url) do |conn|
          STATS.incr_connections_opened
          ch = conn.channel
          ch.prefetch(count: Random.rand(10..100))
          got = Atomic(Int32).new(0)
          ch.basic_consume(Q_STREAM, tag: tag, no_ack: false, block: false, args: args) do |msg|
            begin
              msg.ack
              got.add(1)
              STATS.incr_stream_acked
            rescue
              STATS.incr_errors
            end
          end
          target = 20 + Random.rand(200)
          deadline = Time.instant + 2.seconds
          until STOP.get || got.get >= target || Time.instant >= deadline
            sleep 20.milliseconds
          end
          ch.basic_cancel(tag) rescue nil
          STATS.incr_stream_varied_consumer_cycles
          STATS.incr_connections_closed
        end
      rescue
        STATS.incr_errors
      end
      sleep Random.rand(20..150).milliseconds unless STOP.get
    end
  end

  # Filtered stream consumers. Picks 1-3 filter values from the producer
  # set, randomly chooses 'any' vs 'all' match semantics, and either
  # opts in to x-stream-match-unfiltered (delivering messages without
  # a filter header) or not. Combined with the varied offset args this
  # exercises the StreamFilter / KV / XStreamFilter parse and match
  # paths under churn.
  def self.stream_filtered_consumer(id : Int32)
    cycle = 0_u64
    until STOP.get
      cycle &+= 1
      tag = "#{STREAM_TAG_PREFIX}filtered-#{id}-#{cycle}-#{Random.rand(1_000_000_000)}"
      n_values = 1 + Random.rand(3)
      values = STREAM_FILTER_VALUES.sample(n_values)
      args = random_stream_offset_args
      args["x-stream-filter"] = values.join(",").as(AMQ::Protocol::Field)
      args["x-filter-match-type"] = (Random.rand(2) == 0 ? "any" : "all").as(AMQ::Protocol::Field)
      args["x-stream-match-unfiltered"] = (Random.rand(2) == 0).as(AMQ::Protocol::Field)
      begin
        AMQP::Client.start(CFG.amqp_url) do |conn|
          STATS.incr_connections_opened
          ch = conn.channel
          ch.prefetch(count: Random.rand(10..100))
          got = Atomic(Int32).new(0)
          ch.basic_consume(Q_STREAM, tag: tag, no_ack: false, block: false, args: args) do |msg|
            begin
              msg.ack
              got.add(1)
              STATS.incr_stream_filtered_received
            rescue
              STATS.incr_errors
            end
          end
          target = 20 + Random.rand(200)
          deadline = Time.instant + 2.seconds
          until STOP.get || got.get >= target || Time.instant >= deadline
            sleep 20.milliseconds
          end
          ch.basic_cancel(tag) rescue nil
          STATS.incr_stream_filtered_consumer_cycles
          STATS.incr_connections_closed
        end
      rescue
        STATS.incr_errors
      end
      sleep Random.rand(20..150).milliseconds unless STOP.get
    end
  end

  # -------- Queue pause / resume churn --------
  #
  # Pause a live queue via HTTP, leave it paused briefly while producers
  # pile messages onto it, then resume. Targets the named queues plus
  # the noise pool so consumers and pollers race the pause/resume
  # transitions.
  def self.queue_pause_churn(id : Int32)
    run_until_stop("q-pause-#{id}", sleep_on_error: 200.milliseconds) do
      target = case Random.rand(5)
               when 0 then Q_MAIN
               when 1 then Q_DLQ
               when 2 then Q_POLL
               when 3 then Q_STREAM
               else        "stress.noise.#{Random.rand(NOISE_QUEUES)}"
               end
      path = "/api/queues/#{vhost_path}/#{URI.encode_path_segment(target)}"
      STATS.incr_queues_paused if http_put("#{path}/pause") == 204
      sleep Random.rand(50..400).milliseconds
      STATS.incr_queues_resumed if http_put("#{path}/resume") == 204
      sleep Random.rand(100..600).milliseconds
    end
  end

  # -------- HTTP vhost churn --------

  def self.vhost_churn(id : Int32)
    run_until_stop("vhost-churn-#{id}") do
      name = "stress-vhost-#{id}-#{Time.utc.to_unix_ms}-#{Random.rand(10_000)}"
      enc = URI.encode_path_segment(name)
      code = http_put("/api/vhosts/#{enc}")
      if code == 201 || code == 204
        STATS.incr_vhosts_added
        # Give guest permissions
        perm_body = %({"configure":".*","write":".*","read":".*"})
        http_put("/api/permissions/#{enc}/#{URI.encode_path_segment(CFG.user)}", perm_body)
        # Spawn a quick pub/sub session in this vhost so delete races traffic
        spawn(name: "vhost-traffic-#{name}") do
          begin
            url = "amqp://#{CFG.user}:#{CFG.password}@#{CFG.host}:#{CFG.amqp_port}/#{enc}"
            AMQP::Client.start(url) do |conn|
              ch = conn.channel
              q = ch.queue_declare("vhq", auto_delete: true)
              5.times do
                ch.basic_publish("hi", "", "vhq") rescue break
              end
            end
          rescue
            # vhost may have been deleted under us
          end
        end
        sleep Random.rand(100..500).milliseconds
        del = http_delete("/api/vhosts/#{enc}")
        STATS.incr_vhosts_removed if del == 204
      end
      sleep Random.rand(50..200).milliseconds
    end
  end

  # -------- HTTP connection killer --------

  def self.connection_killer(id : Int32)
    run_until_stop("conn-kill-#{id}", sleep_on_error: 200.milliseconds) do
      sleep Random.rand(200..1000).milliseconds
      code, body = http_get("/api/connections")
      next unless code == 200
      conns = JSON.parse(body).as_a
      next if conns.empty?
      victim = conns.sample
      name = victim["name"]?.try &.as_s
      next unless name
      del = http_delete("/api/connections/#{URI.encode_path_segment(name)}")
      STATS.incr_http_conn_kills if del == 204
    end
  end

  # -------- HTTP publishers --------

  def self.http_publisher(id : Int32)
    run_until_stop("http-pub-#{id}") do
      body = {
        "properties"       => {} of String => JSON::Any,
        "routing_key"      => TOPIC_KEYS.sample,
        "payload"          => "http-msg-#{id}-#{Time.utc.to_unix_ms}",
        "payload_encoding" => "string",
      }.to_json
      path = "/api/exchanges/#{vhost_path}/#{URI.encode_path_segment(EX_TOPIC)}/publish"
      code = http_post(path, body)
      STATS.incr_http_publishes if code == 200
      sleep Random.rand(10..100).milliseconds
    end
  end

  # -------- HTTP list spammer --------

  HTTP_LIST_PATHS = [
    "/api/queues",
    "/api/connections",
    "/api/channels",
    "/api/bindings",
    "/api/exchanges",
    "/api/vhosts",
  ]

  def self.http_list_spammer(id : Int32)
    run_until_stop("http-list-#{id}") do
      path = HTTP_LIST_PATHS.sample
      code, _body = http_get(path)
      STATS.incr_http_lists if code == 200
      sleep Random.rand(5..50).milliseconds
    end
  end

  # -------- Policy churn --------
  #
  # Add and remove policies against live queues (incl. the stream queue and
  # the active main/noise queues). This exercises the policy-application
  # path under load — relevant to the recent serialization work in
  # StreamMessageStore where policy/argument application has to coexist
  # with publishes, acks, and segment rollover.

  # Patterns that target queues currently carrying traffic. A subset of
  # these match the stream queue specifically so we exercise the
  # serialized stream policy-apply path.
  POLICY_PATTERNS = [
    {pattern: "^stress\\.q$", apply_to: "queues", stream: false},
    {pattern: "^stress\\.noise\\.", apply_to: "queues", stream: false},
    {pattern: "^stress\\.tmpq\\.", apply_to: "queues", stream: false},
    {pattern: "^stress\\.poll", apply_to: "queues", stream: false},
    {pattern: "^stress\\.", apply_to: "queues", stream: true},
    {pattern: "^stress\\.stream$", apply_to: "queues", stream: true},
    {pattern: "^stress\\.x\\.", apply_to: "exchanges", stream: false},
    {pattern: "^stress\\.", apply_to: "all", stream: true},
  ]

  # A pool of policy names. Workers pick from this pool so that concurrent
  # PUTs racing the same name (update path) and PUT-vs-DELETE races both
  # happen. The pool is intentionally small to force collisions.
  POLICY_NAMES = %w(churn-a churn-b churn-c churn-d churn-e churn-f churn-g churn-h)

  private def self.build_policy_definition(json : JSON::Builder, allow_stream : Bool) : Nil
    json.object do
      case Random.rand(allow_stream ? 8 : 7)
      when 0
        json.field "max-length", Random.rand(1..200)
        json.field "dead-letter-exchange", EX_DLX if Random.rand(2) == 0
      when 1
        json.field "max-length-bytes", Random.rand(4096..65_536)
        json.field "overflow", "drop-head"
      when 2
        json.field "message-ttl", Random.rand(50..2000)
        json.field "dead-letter-exchange", EX_DLX if Random.rand(2) == 0
      when 3
        json.field "expires", Random.rand(5_000..60_000)
      when 4
        json.field "overflow", "reject-publish"
        json.field "max-length", Random.rand(1..50)
      when 5
        json.field "dead-letter-exchange", EX_DLX
        json.field "dead-letter-routing-key", "via-policy"
      when 6
        json.field "delivery-limit", Random.rand(1..10)
      when 7
        # Stream-applicable: max-length-bytes forces drop_overflow churn.
        json.field "max-length-bytes", Random.rand(1024 * 1024..STREAM_MAX_LENGTH_BYTES)
        json.field "max-age", "#{Random.rand(1..30)}s" if Random.rand(2) == 0
      end
    end
  end

  def self.policy_churn(id : Int32)
    run_until_stop("policy-churn-#{id}") do
      spec = POLICY_PATTERNS.sample
      name = POLICY_NAMES.sample
      body = JSON.build do |json|
        json.object do
          json.field "pattern", spec[:pattern]
          json.field "apply-to", spec[:apply_to]
          json.field "priority", Random.rand(0..10)
          json.field "definition" { build_policy_definition(json, spec[:stream]) }
        end
      end
      path = "/api/policies/#{vhost_path}/#{URI.encode_path_segment(name)}"
      put_code = http_put(path, body)
      if put_code == 201 || put_code == 204
        STATS.incr_policies_added
      else
        Log.debug { "[policy-churn-#{id}] PUT #{path} → #{put_code} body=#{body}" }
      end
      sleep Random.rand(20..300).milliseconds
      del_code = http_delete(path)
      STATS.incr_policies_removed if del_code == 204
      sleep Random.rand(20..200).milliseconds
    end
  end

  # -------- Federation workers --------
  #
  # Each fiber picks one side (vhost A or B) and stays there — alternation
  # comes from spawning workers for both sides. Producers publish to the
  # local `fed.ex` / `fed.qf`; consumers drain the local `fed.q` / `fed.qf`
  # on the opposite side. With bidirectional upstreams that means a message
  # published in vhost A appears on the federated resource in vhost B and
  # vice versa.

  def self.federation_exchange_producer(id : Int32, vhost_key : Symbol)
    vh = FED_VHOSTS[vhost_key]
    with_amqp("fed-ex-prod-#{vhost_key}-#{id}", amqp_url_for(vh)) do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        seq &+= 1
        ch.basic_publish("fed-ex-#{vh}-#{id}-#{seq}", FED_EX, TOPIC_KEYS.sample,
          props: Properties.new(delivery_mode: 1_u8))
        STATS.incr_fed_ex_published
        sleep Random.rand(5..30).milliseconds
      end
    end
  end

  def self.federation_exchange_consumer(id : Int32, vhost_key : Symbol)
    vh = FED_VHOSTS[vhost_key]
    with_amqp("fed-ex-cons-#{vhost_key}-#{id}", amqp_url_for(vh)) do |conn|
      ch = conn.channel
      ch.prefetch(count: 50)
      tag = "fed-ex-cons-#{vh}-#{id}-#{Random.rand(1_000_000)}"
      ch.basic_consume(FED_Q, tag: tag, no_ack: false, block: false) do |msg|
        begin
          msg.ack
          STATS.incr_fed_ex_received
        rescue
          STATS.incr_errors
        end
      end
      until STOP.get || ch.closed? || conn.closed?
        sleep 200.milliseconds
      end
      ch.basic_cancel(tag) rescue nil
    end
  end

  def self.federation_queue_producer(id : Int32, vhost_key : Symbol)
    vh = FED_VHOSTS[vhost_key]
    with_amqp("fed-q-prod-#{vhost_key}-#{id}", amqp_url_for(vh)) do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        seq &+= 1
        ch.basic_publish("fed-q-#{vh}-#{id}-#{seq}", "", FED_QF,
          props: Properties.new(delivery_mode: 1_u8))
        STATS.incr_fed_q_published
        sleep Random.rand(5..30).milliseconds
      end
    end
  end

  def self.federation_queue_consumer(id : Int32, vhost_key : Symbol)
    vh = FED_VHOSTS[vhost_key]
    with_amqp("fed-q-cons-#{vhost_key}-#{id}", amqp_url_for(vh)) do |conn|
      ch = conn.channel
      ch.prefetch(count: 50)
      tag = "fed-q-cons-#{vh}-#{id}-#{Random.rand(1_000_000)}"
      ch.basic_consume(FED_QF, tag: tag, no_ack: false, block: false) do |msg|
        begin
          msg.ack
          STATS.incr_fed_q_received
        rescue
          STATS.incr_errors
        end
      end
      until STOP.get || ch.closed? || conn.closed?
        sleep 200.milliseconds
      end
      ch.basic_cancel(tag) rescue nil
    end
  end

  # Periodically tear down an upstream parameter and re-create it. Exercises
  # the federation link reconnect path (terminate on parameter delete, fresh
  # link on parameter re-add) while exchange/queue federation traffic is in
  # flight.
  def self.federation_churn(id : Int32)
    run_until_stop("fed-churn-#{id}", sleep_on_error: 500.milliseconds) do
      u = FED_UPSTREAMS.sample
      downstream = FED_VHOSTS[u.downstream]
      upstream = FED_VHOSTS[u.upstream]
      del_path = "/api/parameters/federation-upstream/#{vhost_path(downstream)}/#{URI.encode_path_segment(u.name)}"
      STATS.incr_fed_links_removed if http_delete(del_path) == 204
      sleep Random.rand(100..400).milliseconds
      code = put_fed_upstream(downstream, upstream, u.name, u.kind)
      STATS.incr_fed_links_added if code == 201 || code == 204
      STATS.incr_fed_churn_cycles
      sleep Random.rand(500..2000).milliseconds
    end
  end

  # -------- Liveness reporter --------

  def self.liveness_reporter
    last = {
      "pub" => 0_i64, "ack" => 0_i64, "rej" => 0_i64,
      "bnd" => 0_i64, "ex" => 0_i64, "q" => 0_i64,
      "vh" => 0_i64, "kill" => 0_i64,
    }
    last_change = Time.instant
    until STOP.get
      sleep 2.seconds
      cur = {
        "pub" => STATS.published, "ack" => STATS.acked, "rej" => STATS.rejected,
        "bnd" => STATS.bindings_added, "ex" => STATS.exchanges_added, "q" => STATS.queues_added,
        "vh" => STATS.vhosts_added, "kill" => STATS.http_conn_kills,
      }
      changed = cur.any? { |k, v| v != last[k] }
      last_change = Time.instant if changed
      stuck = (Time.instant - last_change).total_seconds
      stuck_marker = stuck >= 10 ? " STUCK=#{stuck.to_i}s" : ""
      log_info(
        "pub=#{STATS.published} ack=#{STATS.acked} rej=#{STATS.rejected} dlx=#{STATS.dead_lettered} " \
        "conf=#{STATS.confirmed}/n#{STATS.confirm_nacked}/t#{STATS.confirm_timeouts}/d#{STATS.confirm_disconnects}(b=#{STATS.batch_confirm_cycles}) " \
        "get=#{STATS.get_ok}/#{STATS.get_empty} " \
        "str=#{STATS.stream_published}/#{STATS.stream_acked}/#{STATS.stream_consumer_cycles} " \
        "str_var=#{STATS.stream_varied_consumer_cycles} " \
        "str_flt=#{STATS.stream_filtered_received}/#{STATS.stream_filtered_consumer_cycles} " \
        "q_pause+/-=#{STATS.queues_paused}/#{STATS.queues_resumed} " \
        "fed_ex=#{STATS.fed_ex_published}/#{STATS.fed_ex_received} " \
        "fed_q=#{STATS.fed_q_published}/#{STATS.fed_q_received} " \
        "fed_ln+/-=#{STATS.fed_links_added}/#{STATS.fed_links_removed}(c=#{STATS.fed_churn_cycles}) " \
        "ex+/-=#{STATS.exchanges_added}/#{STATS.exchanges_removed} " \
        "q+/-=#{STATS.queues_added}/#{STATS.queues_removed} " \
        "bnd+/-=#{STATS.bindings_added}/#{STATS.bindings_removed} " \
        "vh+/-=#{STATS.vhosts_added}/#{STATS.vhosts_removed} " \
        "pol+/-=#{STATS.policies_added}/#{STATS.policies_removed} " \
        "conn+/-=#{STATS.connections_opened}/#{STATS.connections_closed} " \
        "kills=#{STATS.http_conn_kills} purges=#{STATS.purges} " \
        "http_pub=#{STATS.http_publishes} http_lists=#{STATS.http_lists} " \
        "errors=#{STATS.errors}#{stuck_marker}"
      )
      last = cur
    end
  end

  # -------- Orchestration --------

  def self.start_workers
    c = CFG.concurrency

    # Producers: split into fast/slow/bursty
    {EX_TOPIC => :topic, EX_HEADERS => :headers, EX_DIRECT => :direct, EX_FANOUT => :fanout}.each do |ex, kind|
      c.times do |i|
        mode = case i % 3
               when 0 then :fast
               when 1 then :slow
               else        :bursty
               end
        spawn(name: "prod-#{kind}-#{mode}-#{i}") { producer(i, ex, mode.as(Symbol), kind.as(Symbol)) }
      end
      [c // 2, 1].max.times do |i|
        spawn(name: "conf-#{kind}-#{i}") { confirm_producer(i, ex, kind.as(Symbol)) }
      end
      [c // 2, 1].max.times do |i|
        spawn(name: "bconf-#{kind}-#{i}") { batch_confirm_producer(i, ex, kind.as(Symbol)) }
      end
    end

    # Consumers on main queue: mixed fast/slow/bursty plus some rejecting
    (c * 2).times do |i|
      mode = case i % 3
             when 0 then :fast
             when 1 then :slow
             else        :bursty
             end
      reject_rate = i < c // 2 ? 0.1 : 0.0
      spawn(name: "cons-#{mode}-#{i}") { consumer(i, Q_MAIN, mode.as(Symbol), reject_rate) }
    end

    # DLQ drainers
    [c // 2, 1].max.times do |i|
      spawn(name: "dlq-#{i}") { consumer(i, Q_DLQ, :fast) }
    end

    # basic_get pollers
    [c // 2, 1].max.times do |i|
      spawn(name: "poll-#{i}") { poller(i, Q_POLL) }
    end

    # Bind churn (primary targets)
    c.times { |i| spawn(name: "bind-topic-#{i}") { topic_bind_churn(i) } }
    c.times { |i| spawn(name: "bind-headers-#{i}") { headers_bind_churn(i) } }
    c.times { |i| spawn(name: "bind-df-#{i}") { direct_fanout_bind_churn(i) } }

    # Exchange and queue churn
    [c // 2, 1].max.times { |i| spawn(name: "ex-churn-#{i}") { exchange_churn(i) } }
    [c // 2, 1].max.times { |i| spawn(name: "q-churn-#{i}") { queue_churn(i) } }

    # Channel / connection churn
    c.times { |i| spawn(name: "ch-churn-#{i}") { channel_churn(i) } }
    [c // 2, 1].max.times { |i| spawn(name: "conn-churn-#{i}") { connection_churn(i) } }

    # Purge churn
    [c // 4, 1].max.times { |i| spawn(name: "purge-#{i}") { purge_churn(i) } }
    [c // 4, 1].max.times { |i| spawn(name: "q-pause-#{i}") { queue_pause_churn(i) } }

    # Stream workers: producers fill segments, long-lived consumers track
    # offsets, churners cycle through fresh tags to fill the offsets file.
    c.times do |i|
      mode = case i % 3
             when 0 then :fast
             when 1 then :slow
             else        :bursty
             end
      spawn(name: "strp-#{mode}-#{i}") { stream_producer(i, mode.as(Symbol)) }
    end
    [c // 2, 1].max.times { |i| spawn(name: "strc-#{i}") { stream_consumer(i) } }
    [c // 2, 1].max.times { |i| spawn(name: "strc-churn-#{i}") { stream_consumer_churn(i) } }
    [c // 2, 1].max.times { |i| spawn(name: "strc-varied-#{i}") { stream_varied_offset_consumer(i) } }
    [c // 2, 1].max.times { |i| spawn(name: "strc-filtered-#{i}") { stream_filtered_consumer(i) } }

    # Federation workers. Symmetric across the two fed vhosts so messages
    # flow both directions through the bidirectional upstreams.
    fed_per_side = [c // 2, 1].max
    FED_VHOSTS.each_key do |key|
      fed_per_side.times do |i|
        spawn(name: "fed-ex-prod-#{key}-#{i}") { federation_exchange_producer(i, key) }
        spawn(name: "fed-ex-cons-#{key}-#{i}") { federation_exchange_consumer(i, key) }
        spawn(name: "fed-q-prod-#{key}-#{i}") { federation_queue_producer(i, key) }
        spawn(name: "fed-q-cons-#{key}-#{i}") { federation_queue_consumer(i, key) }
      end
    end
    [c // 4, 1].max.times { |i| spawn(name: "fed-churn-#{i}") { federation_churn(i) } }

    # HTTP workers
    [c // 2, 1].max.times { |i| spawn(name: "vhost-churn-#{i}") { vhost_churn(i) } }
    [c // 4, 1].max.times { |i| spawn(name: "conn-kill-#{i}") { connection_killer(i) } }
    c.times { |i| spawn(name: "http-pub-#{i}") { http_publisher(i) } }
    c.times { |i| spawn(name: "http-list-#{i}") { http_list_spammer(i) } }
    [c // 2, 1].max.times { |i| spawn(name: "policy-churn-#{i}") { policy_churn(i) } }

    spawn(name: "liveness") { liveness_reporter }
  end

  def self.main
    parse_args
    Log.setup_from_env(default_level: :warn)
    log_info "Targeting #{CFG.host} AMQP=#{CFG.amqp_port} HTTP=#{CFG.http_port} vhost=#{CFG.vhost}"
    log_info "Duration=#{CFG.duration}s concurrency=#{CFG.concurrency}"

    Signal::INT.trap do
      log_info "SIGINT — stopping"
      STOP.set(true)
    end
    Signal::TERM.trap do
      log_info "SIGTERM — stopping"
      STOP.set(true)
    end

    setup_topology

    start_workers

    deadline = Time.instant + CFG.duration.seconds
    until STOP.get || Time.instant >= deadline
      sleep 500.milliseconds
    end

    log_info "Stopping workers..."
    STOP.set(true)

    # Brief grace period for fibers to settle
    grace_deadline = Time.instant + 5.seconds
    while Time.instant < grace_deadline
      sleep 200.milliseconds
    end

    log_info "Final stats:"
    log_info(
      "  published=#{STATS.published} acked=#{STATS.acked} rejected=#{STATS.rejected} " \
      "dead_lettered=#{STATS.dead_lettered}"
    )
    log_info(
      "  confirmed=#{STATS.confirmed} nacked=#{STATS.confirm_nacked} " \
      "timeouts=#{STATS.confirm_timeouts} disconnects=#{STATS.confirm_disconnects} " \
      "batch_cycles=#{STATS.batch_confirm_cycles}"
    )
    log_info(
      "  stream pub=#{STATS.stream_published} ack=#{STATS.stream_acked} " \
      "consumer_cycles=#{STATS.stream_consumer_cycles} " \
      "varied_cycles=#{STATS.stream_varied_consumer_cycles} " \
      "filtered=#{STATS.stream_filtered_received}/#{STATS.stream_filtered_consumer_cycles}"
    )
    log_info(
      "  queues paused/resumed=#{STATS.queues_paused}/#{STATS.queues_resumed}"
    )
    log_info(
      "  federation ex pub=#{STATS.fed_ex_published} rcv=#{STATS.fed_ex_received} " \
      "q pub=#{STATS.fed_q_published} rcv=#{STATS.fed_q_received} " \
      "links +#{STATS.fed_links_added}/-#{STATS.fed_links_removed} churn=#{STATS.fed_churn_cycles}"
    )
    log_info(
      "  exchanges +#{STATS.exchanges_added}/-#{STATS.exchanges_removed} " \
      "queues +#{STATS.queues_added}/-#{STATS.queues_removed} " \
      "bindings +#{STATS.bindings_added}/-#{STATS.bindings_removed}"
    )
    log_info(
      "  vhosts +#{STATS.vhosts_added}/-#{STATS.vhosts_removed} " \
      "policies +#{STATS.policies_added}/-#{STATS.policies_removed} " \
      "conn +#{STATS.connections_opened}/-#{STATS.connections_closed} " \
      "kills=#{STATS.http_conn_kills} errors=#{STATS.errors}"
    )

    # Aliveness check. After heavy stress the server can be backlogged on
    # I/O (debug logging especially), so allow a generous single timeout
    # rather than a retry loop — the goal is just to confirm the process
    # eventually returns to a responsive state.
    begin
      code, body = http_request("GET", "/api/aliveness-test/#{vhost_path}", read_timeout: 60.seconds)
      if code == 200
        log_info "Aliveness OK"
        exit 0
      else
        log_info "Aliveness FAILED: HTTP #{code} #{body}"
        exit 1
      end
    rescue ex
      log_info "Aliveness FAILED: #{ex.class}: #{ex.message}"
      exit 1
    end
  end
end

Stress.main
