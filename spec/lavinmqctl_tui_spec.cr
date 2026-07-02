require "spec"

{% if flag?(:tui_specs) %}
  require "../src/lavinmqctl/cli"
  require "../src/lavinmqctl/tui"

  class FakeTUIScreen < LavinMQCtl::TUI::Screen
    @cells : Array(Array(Char))

    getter? closed = false

    def initialize(@width : Int32 = 140, @height : Int32 = 36, events = [] of Termisu::Event::Any)
      @events = Deque(Termisu::Event::Any).new(events)
      @cells = Array.new(@height) { Array.new(@width, ' ') }
    end

    def size : {Int32, Int32}
      {@width, @height}
    end

    def poll_event(timeout_ms : Int32) : Termisu::Event::Any?
      @events.shift?
    end

    def clear : Nil
      @cells = Array.new(@height) { Array.new(@width, ' ') }
    end

    def set_cell(
      x : Int32,
      y : Int32,
      char : Char,
      fg : Termisu::Color,
      bg : Termisu::Color,
      attr : Termisu::Attribute,
    ) : Nil
      return if x < 0 || x >= @width || y < 0 || y >= @height

      @cells[y][x] = char
    end

    def render : Nil
    end

    def sync : Nil
    end

    def close : Nil
      @closed = true
    end

    def text : String
      @cells.map(&.join).join("\n")
    end
  end

  private def tui_key(char : Char) : Termisu::Event::Key
    Termisu::Event::Key.new(Termisu::Input::Key.from_char(char))
  end

  private TUI_RESPONSES = {
    "/api/overview" => {
      lavinmq_version: "spec",
      node:            "lavinmq@spec",
      object_totals:   {
        connections: 2,
        channels:    3,
        queues:      4,
        consumers:   5,
        exchanges:   6,
        bindings:    7,
      },
      queue_totals: {
        messages:                    42,
        messages_ready:              39,
        messages_unacknowledged:     3,
        messages_ready_log:          [28, 31, 30, 35, 36, 39],
        messages_unacknowledged_log: [1, 2, 1, 3, 2, 3],
      },
      message_stats: {
        publish_details: {
          rate: 12.5,
          log:  [3.0, 5.1, 8.0, 7.2, 10.3, 12.5],
        },
        deliver_details: {
          rate: 9.7,
          log:  [2.0, 4.1, 6.0, 5.2, 8.3, 9.7],
        },
      },
    }.to_json,
    "/api/queues" => {
      items: [
        {
          vhost:                   "seed",
          name:                    "seed.ready",
          state:                   "running",
          messages:                120,
          messages_ready:          118,
          messages_unacknowledged: 2,
          consumers:               1,
          message_stats:           {
            publish_details: {
              rate: 8.2,
            },
          },
        },
      ],
    }.to_json,
    "/api/connections" => {
      items: [
        {
          vhost:    "seed",
          user:     "guest",
          state:    "running",
          channels: 2,
          recv_oct: 4096,
          send_oct: 8192,
          name:     "127.0.0.1:50000 -> 127.0.0.1:5672",
        },
      ],
    }.to_json,
    "/api/channels" => {
      items: [
        {
          vhost:                   "seed",
          user:                    "guest",
          state:                   "running",
          number:                  1,
          messages_unacknowledged: 3,
          prefetch_count:          25,
          consumer_count:          1,
          name:                    "127.0.0.1:50000 (1)",
        },
      ],
    }.to_json,
    "/api/exchanges" => {
      items: [
        {
          vhost:         "seed",
          name:          "seed.direct",
          type:          "direct",
          durable:       true,
          internal:      false,
          message_stats: {
            publish_in_details: {
              rate: 9.0,
            },
            publish_out_details: {
              rate: 7.0,
            },
          },
        },
      ],
    }.to_json,
    "/api/consumers" => {
      items: [
        {
          consumer_tag:   "seed-consumer-0",
          ack_required:   true,
          prefetch_count: 25,
          queue:          {
            vhost: "seed",
            name:  "seed.work",
          },
          channel_details: {
            name: "127.0.0.1:50000 (1)",
          },
        },
      ],
    }.to_json,
    "/api/vhosts" => [
      {
        name:                    "seed",
        messages:                120,
        messages_ready:          118,
        messages_unacknowledged: 2,
        recv_oct:                4096,
        send_oct:                8192,
        tracing:                 false,
      },
    ].to_json,
    "/api/nodes" => [
      {
        name:         "lavinmq@spec",
        uptime:       7_200_000,
        mem_used:     42_000_000,
        disk_free:    8_500_000_000,
        fd_used:      18,
        sockets_used: 4,
        run_queue:    0,
      },
    ].to_json,
    "/api/parameters" => {
      items: [
        {
          component: "shovel",
          vhost:     "seed",
          name:      "seed-shovel",
          value:     {
            "src-queue":  "seed.shovel.source",
            "dest-queue": "seed.shovel.dest",
            "ack-mode":   "on-confirm",
          },
        },
      ],
    }.to_json,
    "/api/policies" => {
      items: [
        {
          vhost:      "seed",
          name:       "seed-ttl-dlx",
          "apply-to": "queues",
          priority:   10,
          pattern:    "^seed\\.",
          definition: {
            "message-ttl":          60_000,
            "dead-letter-exchange": "seed.dlx",
          },
        },
      ],
    }.to_json,
    "/api/shovels" => [
      {
        vhost:         "seed",
        name:          "seed-shovel",
        state:         "Running",
        error:         nil,
        message_count: 42,
      },
    ].to_json,
    "/api/federation-links" => [
      {
        vhost:     "seed",
        name:      "seed-upstream",
        type:      "exchange",
        resource:  "seed.topic",
        uri:       "amqp://localhost:5672/seed",
        timestamp: "2026-06-29T00:00:00Z",
      },
    ].to_json,
    "/api/users" => [
      {
        name:              "guest",
        tags:              "administrator",
        password_hash:     "********",
        hashing_algorithm: "rabbit_password_hashing_sha256",
      },
    ].to_json,
  }

  private def with_tui_api(status = 200, &)
    server = HTTP::Server.new do |context|
      if body = TUI_RESPONSES[context.request.path]?
        context.response.status_code = status
        context.response.content_type = "application/json"
        context.response.print body if status == 200
      else
        context.response.status_code = 404
      end
    end
    addr = server.bind_tcp("127.0.0.1", 0)
    spawn(name: "tui spec api") { server.listen }
    Fiber.yield

    client = HTTP::Client.new("127.0.0.1", addr.port)
    yield client
  ensure
    client.try &.close
    server.try &.close
  end

  describe LavinMQCtl::TUI do
    {
      {'1', "Overview", "Object totals"},
      {'2', "Queues", "seed.ready"},
      {'3', "Connections", "127.0.0.1:50000"},
      {'4', "Channels", "Unacked"},
      {'5', "Exchanges", "seed.direct"},
      {'6', "Consumers", "seed-consumer-0"},
      {'7', "Vhosts", "seed"},
      {'8', "Nodes", "lavinmq@spec"},
      {'9', "Parameters", "seed-shovel"},
      {'0', "Policies", "seed-ttl-dlx"},
      {'s', "Shovels", "seed-shovel"},
      {'f', "Federation", "seed-upstream"},
      {'u', "Users", "administrator"},
    }.each do |key, page, expected_text|
      it "renders the #{page} page" do
        with_tui_api do |client|
          screen = FakeTUIScreen.new(events: [tui_key(key), tui_key('q')] of Termisu::Event::Any)
          LavinMQCtl::TUI.new(client, 60.0, screen).start

          screen.text.should contain(page)
          screen.text.should contain(expected_text)
          screen.closed?.should be_true
        end
      end
    end

    it "renders HTTP errors in the footer" do
      with_tui_api(status: 401) do |client|
        screen = FakeTUIScreen.new(events: [tui_key('q')] of Termisu::Event::Any)
        LavinMQCtl::TUI.new(client, 60.0, screen).start

        screen.text.should contain("overview: HTTP 401 UNAUTHORIZED")
        screen.closed?.should be_true
      end
    end
  end
{% end %}
