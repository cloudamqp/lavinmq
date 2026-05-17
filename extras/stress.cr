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
                     bindings_added bindings_removed
                     exchanges_added exchanges_removed
                     queues_added queues_removed
                     vhosts_added vhosts_removed
                     policies_added policies_removed
                     connections_opened connections_closed
                     channels_opened channels_closed
                     http_publishes http_lists http_conn_kills
                     stream_published stream_acked stream_consumer_cycles
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

  TOPIC_KEYS   = %w(a.b.c a.b.d a.x.y x.b.c k.l.m foo.bar foo.baz hot.cold ping.pong red.green.blue)
  HEADER_TYPES = %w(a b c d)

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

  def self.with_amqp(name : String, &)
    until STOP.get
      begin
        AMQP::Client.start(CFG.amqp_url) do |conn|
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

  # mode: :fast | :slow | :bursty
  def self.producer(id : Int32, exchange : String, mode : Symbol, kind : Symbol)
    with_amqp("prod-#{kind}-#{mode}-#{id}") do |conn|
      ch = conn.channel
      seq = 0_u64
      while !STOP.get && !ch.closed?
        seq &+= 1
        body = "msg-#{kind}-#{mode}-#{id}-#{seq}"
        rkey = ""
        args = Properties.new(delivery_mode: 1_u8)
        # 10% messages carry per-message TTL for DLX path
        if Random.rand(10) == 0
          args = Properties.new(delivery_mode: 1_u8, expiration: Random.rand(500).to_s)
        end
        case kind
        when :topic
          rkey = TOPIC_KEYS.sample
          ch.basic_publish(body, exchange, rkey, props: args)
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
        ch.basic_publish(STREAM_PAYLOAD, "", Q_STREAM, props: Properties.new(delivery_mode: 1_u8))
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
        "get=#{STATS.get_ok}/#{STATS.get_empty} " \
        "str=#{STATS.stream_published}/#{STATS.stream_acked}/#{STATS.stream_consumer_cycles} " \
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
      "  stream pub=#{STATS.stream_published} ack=#{STATS.stream_acked} " \
      "consumer_cycles=#{STATS.stream_consumer_cycles}"
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
