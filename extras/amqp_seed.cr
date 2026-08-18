# Seed a LavinMQ broker with topology and traffic useful for exercising the
# HTTP management API and lavinmqctl TUI.
#
# Usage:
#   crystal run extras/amqp_seed.cr -- --messages 200
#   crystal run extras/amqp_seed.cr -- --live

require "amqp-client"
require "base64"
require "http/client"
require "json"
require "option_parser"
require "uri"

module AMQPSeed
  alias Arguments = AMQ::Protocol::Table
  alias Properties = AMQ::Protocol::Properties

  class Config
    property host = "localhost"
    property amqp_port = 5672
    property http_port = 15672
    property user = "guest"
    property password = "guest"
    property vhost = "seed"
    property messages = 200
    property? live = false
    property consumers = 2

    def http_base : String
      "http://#{@host}:#{@http_port}"
    end
  end

  CFG  = Config.new
  STOP = Atomic(Bool).new(false)

  EX_DIRECT  = "seed.direct"
  EX_TOPIC   = "seed.topic"
  EX_FANOUT  = "seed.fanout"
  EX_HEADERS = "seed.headers"
  EX_DLX     = "seed.dlx"

  Q_READY         = "seed.ready"
  Q_WORK          = "seed.work"
  Q_DLX           = "seed.dlx"
  Q_TTL           = "seed.ttl"
  Q_PRIORITY      = "seed.priority"
  Q_STREAM        = "seed.stream"
  Q_SHOVEL_SOURCE = "seed.shovel.source"
  Q_SHOVEL_DEST   = "seed.shovel.dest"

  def self.parse_args
    OptionParser.parse do |p|
      p.banner = "Usage: amqp_seed [options]"
      p.on("--host=HOST", "Broker host (default localhost)") { |v| CFG.host = v }
      p.on("--amqp-port=PORT", "AMQP port (default 5672)") { |v| CFG.amqp_port = v.to_i }
      p.on("--http-port=PORT", "HTTP API port (default 15672)") { |v| CFG.http_port = v.to_i }
      p.on("--user=USER", "Username (default guest)") { |v| CFG.user = v }
      p.on("--password=PASS", "Password (default guest)") { |v| CFG.password = v }
      p.on("--vhost=VHOST", "Seed vhost (default seed)") { |v| CFG.vhost = v }
      p.on("--messages=N", "Messages to publish (default 200)") { |v| CFG.messages = v.to_i }
      p.on("--consumers=N", "Live consumers to keep open with --live (default 2)") { |v| CFG.consumers = v.to_i }
      p.on("--live", "Keep AMQP connections, channels, and consumers visible") { CFG.live = true }
      p.on("-h", "--help", "Show help") { puts p; exit 0 }
    end
  end

  def self.log(event : String, **fields)
    STDERR << "at=info event=" << event
    fields.each do |key, value|
      STDERR << ' ' << key << '=' << value
    end
    STDERR << '\n'
  end

  def self.vhost_path(vhost : String = CFG.vhost) : String
    URI.encode_path_segment(vhost)
  end

  def self.amqp_url(vhost : String = CFG.vhost, name : String? = nil) : String
    path = URI.encode_path_segment(vhost)
    query = name ? "?name=#{URI.encode_www_form(name)}" : ""
    "amqp://#{CFG.user}:#{CFG.password}@#{CFG.host}:#{CFG.amqp_port}/#{path}#{query}"
  end

  def self.http_request(method : String, path : String, body : String? = nil) : {Int32, String}
    uri = URI.parse("#{CFG.http_base}#{path}")
    headers = HTTP::Headers{
      "Authorization" => "Basic #{Base64.strict_encode("#{CFG.user}:#{CFG.password}")}",
      "Content-Type"  => "application/json",
    }
    client = HTTP::Client.new(uri)
    client.connect_timeout = 5.seconds
    client.read_timeout = 15.seconds
    response = client.exec(method, uri.request_target, headers, body)
    {response.status_code, response.body.to_s}
  ensure
    client.try &.close
  end

  def self.expect(method : String, path : String, body : String? = nil, ok = {200, 201, 204})
    code, response = http_request(method, path, body)
    unless ok.includes?(code)
      raise "#{method} #{path} failed status=#{code} body=#{response.inspect}"
    end
    log "http", method: method, path: path, status: code
  end

  def self.seed_api
    expect "PUT", "/api/vhosts/#{vhost_path}"
    expect "PUT", "/api/vhosts/#{vhost_path("#{CFG.vhost}-alt")}"

    expect "PUT", "/api/users/seed-admin", {password: "seed-admin", tags: "administrator"}.to_json
    expect "PUT", "/api/users/seed-app", {password: "seed-app", tags: "management"}.to_json

    perm = {configure: ".*", write: ".*", read: ".*"}.to_json
    expect "PUT", "/api/permissions/#{vhost_path}/#{URI.encode_path_segment(CFG.user)}", perm
    expect "PUT", "/api/permissions/#{vhost_path}/seed-app", perm

    expect "PUT", "/api/vhost-limits/#{vhost_path}/max-connections", {value: 100}.to_json
    expect "PUT", "/api/vhost-limits/#{vhost_path}/max-queues", {value: 1000}.to_json

    ttl_policy = {
      "pattern"    => "^seed\\.",
      "apply-to"   => "queues",
      "priority"   => 10,
      "definition" => {
        "message-ttl"          => 60_000,
        "dead-letter-exchange" => EX_DLX,
      },
    }
    expect "PUT", "/api/policies/#{vhost_path}/seed-ttl-dlx", ttl_policy.to_json

    operator_policy = {
      "pattern"    => "^seed\\.",
      "apply-to"   => "queues",
      "priority"   => 1,
      "definition" => {
        "max-length" => 10_000,
      },
    }
    expect "PUT", "/api/operator-policies/#{vhost_path}/seed-operator-limits", operator_policy.to_json

    uri = amqp_url
    shovel = {
      "value" => {
        "src-uri"            => uri,
        "dest-uri"           => uri,
        "src-queue"          => Q_SHOVEL_SOURCE,
        "dest-queue"         => Q_SHOVEL_DEST,
        "src-prefetch-count" => 25,
        "src-delete-after"   => "never",
        "reconnect-delay"    => 5,
        "ack-mode"           => "on-confirm",
      },
    }
    expect "PUT", "/api/parameters/shovel/#{vhost_path}/seed-shovel", shovel.to_json

    federation = {
      "value" => {
        "uri"             => uri,
        "prefetch-count"  => 25,
        "reconnect-delay" => 5,
        "ack-mode"        => "on-confirm",
        "exchange"        => EX_TOPIC,
        "max-hops"        => 1,
      },
    }
    expect "PUT", "/api/parameters/federation-upstream/#{vhost_path}/seed-upstream", federation.to_json
  end

  def self.seed_topology
    AMQP::Client.start(amqp_url(CFG.vhost, "amqp-seed-topology")) do |conn|
      ch = conn.channel
      ch.exchange_declare EX_DIRECT, "direct", durable: true
      ch.exchange_declare EX_TOPIC, "topic", durable: true
      ch.exchange_declare EX_FANOUT, "fanout", durable: true
      ch.exchange_declare EX_HEADERS, "headers", durable: true
      ch.exchange_declare EX_DLX, "fanout", durable: true

      ch.queue_declare Q_READY, durable: true
      ch.queue_declare Q_WORK, durable: true, args: Arguments.new({
        "x-dead-letter-exchange" => EX_DLX.as(AMQ::Protocol::Field),
      })
      ch.queue_declare Q_DLX, durable: true
      ch.queue_declare Q_TTL, durable: true, args: Arguments.new({
        "x-message-ttl"          => 60_000_i64.as(AMQ::Protocol::Field),
        "x-dead-letter-exchange" => EX_DLX.as(AMQ::Protocol::Field),
      })
      ch.queue_declare Q_PRIORITY, durable: true, args: Arguments.new({
        "x-max-priority" => 10_i64.as(AMQ::Protocol::Field),
      })
      ch.queue_declare Q_STREAM, durable: true, args: Arguments.new({
        "x-queue-type"       => "stream".as(AMQ::Protocol::Field),
        "x-max-length-bytes" => 16_777_216_i64.as(AMQ::Protocol::Field),
      })
      ch.queue_declare Q_SHOVEL_SOURCE, durable: true
      ch.queue_declare Q_SHOVEL_DEST, durable: true

      ch.queue_bind Q_READY, EX_DIRECT, "ready"
      ch.queue_bind Q_WORK, EX_TOPIC, "work.#"
      ch.queue_bind Q_TTL, EX_FANOUT, ""
      ch.queue_bind Q_PRIORITY, EX_DIRECT, "priority"
      ch.queue_bind Q_DLX, EX_DLX, ""
      ch.queue_bind Q_STREAM, EX_TOPIC, "stream.#"
      ch.queue_bind Q_SHOVEL_SOURCE, EX_DIRECT, "shovel"
      ch.queue_bind Q_SHOVEL_DEST, EX_DIRECT, "shovel.dest"
      ch.queue_bind Q_READY, EX_HEADERS, "", args: Arguments.new({
        "x-match" => "any".as(AMQ::Protocol::Field),
        "kind"    => "ready".as(AMQ::Protocol::Field),
      })

      ch.exchange_bind EX_TOPIC, EX_FANOUT, "#"
    end
    log "topology_seeded", vhost: CFG.vhost
  end

  def self.publish_messages
    AMQP::Client.start(amqp_url(CFG.vhost, "amqp-seed-publisher")) do |conn|
      ch = conn.channel
      CFG.messages.times do |i|
        props = Properties.new(
          content_type: "text/plain",
          delivery_mode: 2_u8,
          message_id: "seed-#{i}",
          timestamp: Time.utc,
          app_id: "amqp_seed",
        )
        case i % 6
        when 0
          ch.basic_publish("ready #{i}", EX_DIRECT, "ready", props: props)
        when 1
          ch.basic_publish("work #{i}", EX_TOPIC, "work.fast", props: props)
        when 2
          ch.basic_publish("ttl #{i}", EX_FANOUT, "", props: props)
        when 3
          priority_props = Properties.new(
            content_type: "text/plain",
            delivery_mode: 2_u8,
            priority: (i % 10).to_u8,
            message_id: "seed-priority-#{i}",
            app_id: "amqp_seed",
          )
          ch.basic_publish("priority #{i}", EX_DIRECT, "priority", props: priority_props)
        when 4
          headers = Arguments.new({
            "kind" => "ready".as(AMQ::Protocol::Field),
          })
          header_props = Properties.new(
            content_type: "text/plain",
            delivery_mode: 2_u8,
            headers: headers,
            message_id: "seed-header-#{i}",
            app_id: "amqp_seed",
          )
          ch.basic_publish("headers #{i}", EX_HEADERS, "", props: header_props)
        else
          ch.basic_publish("stream #{i}", EX_TOPIC, "stream.seed", props: props)
        end
      end
    end
    log "messages_published", count: CFG.messages
  end

  def self.publish_http_message
    body = {
      "properties" => {
        "content_type"  => "text/plain",
        "delivery_mode" => 2,
        "app_id"        => "amqp_seed",
      },
      "routing_key"      => "ready",
      "payload"          => "published through HTTP API",
      "payload_encoding" => "string",
    }
    expect "POST", "/api/exchanges/#{vhost_path}/#{URI.encode_path_segment(EX_DIRECT)}/publish", body.to_json, ok: {200}
  end

  def self.keep_live_consumers
    connections = [] of AMQP::Client::Connection
    CFG.consumers.times do |i|
      conn = AMQP::Client.new(amqp_url(CFG.vhost, "amqp-seed-consumer-#{i}")).connect
      connections << conn
      ch = conn.channel
      ch.prefetch(25)
      queue = i.even? ? Q_WORK : Q_STREAM
      ch.basic_consume(queue, "seed-consumer-#{i}", no_ack: false, block: false) do |_msg|
        # Leave deliveries unacked so queue, channel, consumer, and unacked
        # API views have something to show.
      end
    end

    log "live_consumers_started", consumers: CFG.consumers
    Signal::INT.trap { STOP.set(true) }
    Signal::TERM.trap { STOP.set(true) }
    until STOP.get
      sleep 1.second
    end
  ensure
    connections.try &.each(&.close)
  end

  def self.run
    parse_args
    seed_api
    seed_topology
    publish_messages
    publish_http_message
    log "seed_complete", vhost: CFG.vhost, live: CFG.live?
    keep_live_consumers if CFG.live?
  end
end

AMQPSeed.run
