require "spec"

# termisu and systemd.cr (required by the launcher specs) bind poll(2) with
# different signatures and can't be compiled into the same binary, so these
# specs only run with -Dtui_specs, which `make test` passes in a separate run
{% skip_file unless flag?(:tui_specs) %}

require "../src/lavinmqctl/cli"
require "../src/lavinmqctl/tui"

class FakeTUIScreen < LavinMQCtl::TUI::Screen
  @cells : Array(Array(Char))
  @colors : Array(Array(Termisu::Color))

  getter? closed = false

  def initialize(@width : Int32 = 140, @height : Int32 = 36, events = [] of Termisu::Event::Any)
    @events = Deque(Termisu::Event::Any).new(events)
    @cells = Array.new(@height) { Array.new(@width, ' ') }
    @colors = Array.new(@height) { Array.new(@width, Termisu::Color.default) }
  end

  def size : {Int32, Int32}
    {@width, @height}
  end

  def poll_event(timeout_ms : Int32) : Termisu::Event::Any?
    @events.shift?
  end

  def clear : Nil
    @cells = Array.new(@height) { Array.new(@width, ' ') }
    @colors = Array.new(@height) { Array.new(@width, Termisu::Color.default) }
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
    @colors[y][x] = fg
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

  # Row of the highest braille graph cell drawn in *color*
  def top_row(color : Termisu::Color) : Int32?
    @cells.each_with_index do |row, y|
      row.each_with_index do |c, x|
        return y if ('⠁'..'⣿').includes?(c) && @colors[y][x] == color
      end
    end
  end
end

private def tui_key(char : Char) : Termisu::Event::Key
  Termisu::Event::Key.new(Termisu::Input::Key.from_char(char))
end

# Returns a key that maps to no page on every poll, then quits
class KeyRepeatTUIScreen < FakeTUIScreen
  def initialize(@presses : Int32)
    super()
  end

  def poll_event(timeout_ms : Int32) : Termisu::Event::Any?
    sleep 10.milliseconds
    @presses -= 1
    key = @presses > 0 ? 'x' : 'q'
    Termisu::Event::Key.new(Termisu::Input::Key.from_char(key))
  end
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
      messages:                    4200,
      messages_ready:              3900,
      messages_unacknowledged:     300,
      messages_ready_log:          [2800, 3100, 3000, 3500, 3600, 3900],
      messages_unacknowledged_log: [100, 200, 100, 300, 200, 300],
    },
    message_stats: {
      publish_details: {
        rate: 12.5,
        log:  [3.0, 5.1, 8.0, 7.2, 10.3, 12.5],
      },
      deliver_get_details: {
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
    filtered_count: 25,
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
      {
        component: "shovel",
        vhost:     "seed",
        name:      "seed-exchange-shovel",
        value:     {
          "src-uri":      "amqp://guest:s3cret@localhost:5672/seed",
          "src-exchange": "seed.topic",
          "dest-uri":     "amqp://guest:s3cret@localhost:5672/seed",
          "dest-queue":   "seed.shovel.dest",
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
      uri:       "amqp://guest:s3cret@localhost:5672/seed",
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

private def with_tui_api(status = 200, responses = TUI_RESPONSES, &)
  requests = [] of String
  server = HTTP::Server.new do |context|
    requests << context.request.path
    if body = responses[context.request.path]?
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
  yield client, requests
ensure
  client.try &.close
  server.try &.close
end

describe LavinMQCtl::TUI do
  {
    {'1', "Overview", ["Object totals"]},
    {'2', "Queues", ["seed.ready", "(1 of 25)"]},
    {'3', "Connections", ["127.0.0.1:50000", "8.0KiB"]},
    {'4', "Channels", ["Unacked"]},
    {'5', "Exchanges", ["seed.direct"]},
    {'6', "Consumers", ["seed-consumer-0"]},
    {'7', "Vhosts", ["seed"]},
    {'8', "Nodes", ["7.9GiB"]}, # disk_free is above Int32::MAX
    {'9', "Parameters", ["seed-shovel"]},
    {'0', "Policies", ["seed-ttl-dlx"]},
    {'s', "Shovels", ["seed-shovel"]},
    {'f', "Federation", ["seed-upstream", "amqp://guest:***@localhost:5672/seed"]},
    {'u', "Users", ["administrator"]},
  }.each do |key, page, expected_texts|
    it "renders the #{page} page" do
      with_tui_api do |client|
        screen = FakeTUIScreen.new(events: [tui_key(key), tui_key('q')] of Termisu::Event::Any)
        LavinMQCtl::TUI.new(client, 60.0, screen).start

        screen.text.should contain(page)
        expected_texts.each { |text| screen.text.should contain(text) }
        screen.text.should_not contain("s3cret")
        screen.closed?.should be_true
      end
    end
  end

  it "shows message counts as numbers, not bytes" do
    with_tui_api do |client|
      screen = FakeTUIScreen.new(events: [tui_key('q')] of Termisu::Event::Any)
      LavinMQCtl::TUI.new(client, 60.0, screen).start

      screen.text.should_not contain("3.8KiB")
    end
  end

  it "shows both series of a graph when they are equal" do
    overview = JSON.parse(TUI_RESPONSES["/api/overview"]).as_h
    rate = JSON.parse({rate: 5.0, log: [5.0, 5.0, 5.0]}.to_json)
    overview["message_stats"] = JSON::Any.new({"publish_details" => rate, "deliver_get_details" => rate})
    responses = TUI_RESPONSES.merge({"/api/overview" => overview.to_json})
    with_tui_api(responses: responses) do |client|
      screen = FakeTUIScreen.new(events: [tui_key('q')] of Termisu::Event::Any)
      LavinMQCtl::TUI.new(client, 60.0, screen).start

      legend_row = screen.text.lines.index!(&.includes?("publish"))
      publish_top = screen.top_row(LavinMQCtl::TUI::CYAN).should_not be_nil
      deliver_top = screen.top_row(LavinMQCtl::TUI::MAGENTA).should_not be_nil
      publish_top.should be < legend_row
      deliver_top.should be < legend_row
    end
  end

  it "draws both series of a graph on the same scale" do
    with_tui_api do |client|
      screen = FakeTUIScreen.new(events: [tui_key('q')] of Termisu::Event::Any)
      LavinMQCtl::TUI.new(client, 60.0, screen).start

      # Publish peaks at 12.5/s and deliver at 9.7/s, so publish reaches higher
      publish_top = screen.top_row(LavinMQCtl::TUI::CYAN).should_not be_nil
      deliver_top = screen.top_row(LavinMQCtl::TUI::MAGENTA).should_not be_nil
      publish_top.should be < deliver_top
    end
  end

  it "fits the overview panels in a small terminal" do
    with_tui_api do |client|
      screen = FakeTUIScreen.new(width: 90, height: 24, events: [tui_key('q')] of Termisu::Event::Any)
      LavinMQCtl::TUI.new(client, 60.0, screen).start

      lines = screen.text.lines
      lines.select(&.includes?('╰')).each(&.should_not(match(/\w/)))
      screen.text.should contain("max 12.5/s")
    end
  end

  it "doesn't read a timed out response as the answer to the next request" do
    stalled = false
    server = HTTP::Server.new do |context|
      context.response.content_type = "application/json"
      if context.request.path == "/api/overview" && !stalled
        stalled = true
        sleep 300.milliseconds
        context.response.print({lavinmq_version: "stale"}.to_json)
      else
        context.response.print TUI_RESPONSES[context.request.path]? || "{}"
      end
    end
    addr = server.bind_tcp("127.0.0.1", 0)
    spawn(name: "tui spec api") { server.listen }
    client = HTTP::Client.new("127.0.0.1", addr.port)
    client.read_timeout = 200.milliseconds

    screen = FakeTUIScreen.new(events: [tui_key('1'), tui_key('q')] of Termisu::Event::Any)
    LavinMQCtl::TUI.new(client, 60.0, screen).start

    screen.text.should contain("vspec")
  ensure
    client.try &.close
    server.try &.close
  end

  it "keeps refreshing while keys are pressed" do
    with_tui_api do |client, requests|
      screen = KeyRepeatTUIScreen.new(presses: 30)
      LavinMQCtl::TUI.new(client, 0.05, screen).start

      requests.count("/api/overview").should be >= 3
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
