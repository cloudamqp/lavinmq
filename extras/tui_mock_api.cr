require "http/server"
require "json"
require "option_parser"

module TUIMockAPI
  class Config
    property host = "127.0.0.1"
    property port = 15692
  end

  CFG = Config.new

  def self.parse_args
    OptionParser.parse do |p|
      p.banner = "Usage: tui_mock_api [options]"
      p.on("--host=HOST", "Bind host (default 127.0.0.1)") { |v| CFG.host = v }
      p.on("--port=PORT", "Bind port (default 15692)") { |v| CFG.port = v.to_i }
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

  def self.paginated(json : JSON::Builder, total_count : Int32)
    json.field "page", 1
    json.field "page_size", 20
    json.field "page_count", 1
    json.field "total_count", total_count
    json.field "filtered_count", total_count
  end

  def self.stats(json : JSON::Builder, rate : Float64)
    json.object do
      json.field "rate", rate
    end
  end

  def self.overview
    JSON.build do |json|
      json.object do
        json.field "lavinmq_version", "mock"
        json.field "node", "lavinmq@mock"
        json.field "uptime", 7_200_000
        json.field "object_totals" do
          json.object do
            json.field "connections", 3
            json.field "channels", 5
            json.field "queues", 7
            json.field "consumers", 4
            json.field "exchanges", 6
            json.field "bindings", 12
          end
        end
        json.field "queue_totals" do
          json.object do
            json.field "messages", 183
            json.field "messages_ready", 166
            json.field "messages_unacknowledged", 17
          end
        end
        json.field "message_stats" do
          json.object do
            json.field "publish_details" { stats(json, 23.4) }
          end
        end
      end
    end
  end

  def self.queues
    queue_items([
      {"seed", "seed.ready", "running", 120, 120, 0, 0, 8.2},
      {"seed", "seed.work", "running", 41, 24, 17, 2, 12.4},
      {"seed", "seed.stream", "running", 22, 22, 0, 1, 2.8},
    ])
  end

  def self.queue_items(items)
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            items.each do |vhost, name, state, messages, ready, unacked, consumers, rate|
              json.object do
                json.field "vhost", vhost
                json.field "name", name
                json.field "state", state
                json.field "messages", messages
                json.field "messages_ready", ready
                json.field "messages_unacknowledged", unacked
                json.field "consumers", consumers
                json.field "message_stats" do
                  json.object do
                    json.field "publish_details" { stats(json, rate.as(Float64)) }
                  end
                end
              end
            end
          end
        end
        paginated(json, items.size)
      end
    end
  end

  def self.connections
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            3.times do |i|
              json.object do
                json.field "vhost", "seed"
                json.field "user", i.zero? ? "guest" : "seed-app"
                json.field "state", "running"
                json.field "channels", i + 1
                json.field "recv_oct", 4096 * (i + 1)
                json.field "send_oct", 8192 * (i + 1)
                json.field "name", "127.0.0.1:#{50_000 + i} -> 127.0.0.1:5672"
              end
            end
          end
        end
        paginated(json, 3)
      end
    end
  end

  def self.channels
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            5.times do |i|
              json.object do
                json.field "vhost", "seed"
                json.field "user", "seed-app"
                json.field "state", "running"
                json.field "number", i + 1
                json.field "messages_unacknowledged", i == 1 ? 17 : 0
                json.field "prefetch_count", 25
                json.field "consumer_count", i < 2 ? 1 : 0
                json.field "name", "127.0.0.1:#{50_000 + i} (#{i + 1})"
              end
            end
          end
        end
        paginated(json, 5)
      end
    end
  end

  def self.exchanges
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            [{"seed.direct", "direct"}, {"seed.topic", "topic"}, {"seed.fanout", "fanout"}, {"seed.headers", "headers"}].each_with_index do |(name, type), i|
              json.object do
                json.field "vhost", "seed"
                json.field "name", name
                json.field "type", type
                json.field "durable", true
                json.field "internal", false
                json.field "message_stats" do
                  json.object do
                    json.field "publish_in_details" { stats(json, (i + 1) * 3.0) }
                    json.field "publish_out_details" { stats(json, (i + 1) * 2.0) }
                  end
                end
              end
            end
          end
        end
        paginated(json, 4)
      end
    end
  end

  def self.consumers
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            4.times do |i|
              json.object do
                json.field "consumer_tag", "seed-consumer-#{i}"
                json.field "ack_required", true
                json.field "prefetch_count", 25
                json.field "queue" do
                  json.object do
                    json.field "vhost", "seed"
                    json.field "name", i.even? ? "seed.work" : "seed.stream"
                  end
                end
                json.field "channel_details" do
                  json.object do
                    json.field "name", "127.0.0.1:#{50_000 + i} (#{i + 1})"
                  end
                end
              end
            end
          end
        end
        paginated(json, 4)
      end
    end
  end

  def self.vhosts
    JSON.build do |json|
      json.array do
        {"seed", "seed-alt", "/"}.each_with_index do |name, i|
          json.object do
            json.field "name", name
            json.field "messages", 180 - i
            json.field "messages_ready", 160 - i
            json.field "messages_unacknowledged", i == 0 ? 17 : 0
            json.field "recv_oct", 12_000 * (i + 1)
            json.field "send_oct", 24_000 * (i + 1)
            json.field "tracing", false
          end
        end
      end
    end
  end

  def self.nodes
    JSON.build do |json|
      json.array do
        json.object do
          json.field "name", "lavinmq@mock"
          json.field "uptime", 7_200_000
          json.field "mem_used", 42_000_000
          json.field "disk_free", 8_500_000_000
          json.field "fd_used", 18
          json.field "sockets_used", 6
          json.field "run_queue", 0
        end
      end
    end
  end

  def self.parameters
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            json.object do
              json.field "component", "shovel"
              json.field "vhost", "seed"
              json.field "name", "seed-shovel"
              json.field "value" do
                json.object do
                  json.field "src-queue", "seed.shovel.source"
                  json.field "dest-queue", "seed.shovel.dest"
                  json.field "ack-mode", "on-confirm"
                end
              end
            end
            json.object do
              json.field "component", "federation-upstream"
              json.field "vhost", "seed"
              json.field "name", "seed-upstream"
              json.field "value" do
                json.object do
                  json.field "uri", "amqp://guest:guest@localhost:5672/seed"
                end
              end
            end
          end
        end
        paginated(json, 2)
      end
    end
  end

  def self.policies
    JSON.build do |json|
      json.object do
        json.field "items" do
          json.array do
            json.object do
              json.field "vhost", "seed"
              json.field "name", "seed-ttl-dlx"
              json.field "apply-to", "queues"
              json.field "priority", 10
              json.field "pattern", "^seed\\."
              json.field "definition" do
                json.object do
                  json.field "message-ttl", 60000
                  json.field "dead-letter-exchange", "seed.dlx"
                end
              end
            end
          end
        end
        paginated(json, 1)
      end
    end
  end

  def self.shovels
    JSON.build do |json|
      json.array do
        json.object do
          json.field "vhost", "seed"
          json.field "name", "seed-shovel"
          json.field "state", "Running"
          json.field "error", nil
          json.field "message_count", 42
        end
      end
    end
  end

  def self.federation_links
    JSON.build do |json|
      json.array do
        json.object do
          json.field "vhost", "seed"
          json.field "name", "seed-upstream"
          json.field "type", "exchange"
          json.field "resource", "seed.topic"
          json.field "uri", "amqp://localhost:5672/seed"
          json.field "timestamp", "2026-06-29T00:00:00Z"
        end
      end
    end
  end

  def self.users
    JSON.build do |json|
      json.array do
        json.object do
          json.field "name", "guest"
          json.field "tags", "administrator"
          json.field "password_hash", "********"
          json.field "hashing_algorithm", "rabbit_password_hashing_sha256"
        end
        json.object do
          json.field "name", "seed-app"
          json.field "tags", "management"
          json.field "password_hash", "********"
          json.field "hashing_algorithm", "rabbit_password_hashing_sha256"
        end
      end
    end
  end

  ROUTES = {
    "/api/overview"          => -> { TUIMockAPI.overview },
    "/api/queues"            => -> { TUIMockAPI.queues },
    "/api/connections"       => -> { TUIMockAPI.connections },
    "/api/channels"          => -> { TUIMockAPI.channels },
    "/api/exchanges"         => -> { TUIMockAPI.exchanges },
    "/api/consumers"         => -> { TUIMockAPI.consumers },
    "/api/vhosts"            => -> { TUIMockAPI.vhosts },
    "/api/nodes"             => -> { TUIMockAPI.nodes },
    "/api/parameters"        => -> { TUIMockAPI.parameters },
    "/api/parameters/shovel" => -> { TUIMockAPI.parameters },
    "/api/policies"          => -> { TUIMockAPI.policies },
    "/api/shovels"           => -> { TUIMockAPI.shovels },
    "/api/federation-links"  => -> { TUIMockAPI.federation_links },
    "/api/users"             => -> { TUIMockAPI.users },
  }

  def self.response_for(path : String) : String?
    ROUTES[path]?.try &.call
  end

  def self.run
    parse_args
    server = HTTP::Server.new do |context|
      if body = response_for(context.request.path)
        context.response.content_type = "application/json"
        context.response.print body
      else
        context.response.status_code = 404
        context.response.content_type = "application/json"
        context.response.print({error: "not found"}.to_json)
      end
    end
    server.bind_tcp(CFG.host, CFG.port)
    log "started", host: CFG.host, port: CFG.port
    server.listen
  end
end

TUIMockAPI.run
