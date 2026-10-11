require "spec"
require "../src/lavinmqctl/cli"
require "../src/lavinmqctl/tui"

alias TUI = LavinMQCtl::TUI

class FakeTUIScreen < TUI::Screen
  # The right half of a wide character
  WIDE_RIGHT = '\0'

  @cells : Array(Array(Char))
  @colors : Array(Array(TUI::Color))

  getter? closed = false

  # Once the events are used up it waits *quit_after*, refreshing on
  # schedule, and then quits
  def initialize(@width : Int32 = 140, @height : Int32 = 36, events = [] of TUI::Event, @quit_after : Time::Span? = nil)
    @events = Deque(TUI::Event).new(events)
    @cells = Array.new(@height) { Array.new(@width, ' ') }
    @colors = Array.new(@height) { Array.new(@width, TUI::WHITE) }
    @started = Time.instant
  end

  def size : {Int32, Int32}
    {@width, @height}
  end

  # One event per wait, as if typed one at a time
  def poll_event(timeout : Time::Span) : TUI::Event?
    return if timeout.zero?
    if event = @events.shift?
      resize(event.width, event.height) if event.is_a?(TUI::ResizeEvent)
      return event
    end
    quit_after = @quit_after
    return unless quit_after
    remaining = quit_after - (Time.instant - @started)
    return TUI::KeyEvent.char('q') if remaining <= Time::Span.zero
    sleep({timeout, remaining}.min)
    nil
  end

  def clear : Nil
    @cells = Array.new(@height) { Array.new(@width, ' ') }
  end

  private def resize(@width : Int32, @height : Int32)
    @cells = Array.new(@height) { Array.new(@width, ' ') }
    @colors = Array.new(@height) { Array.new(@width, TUI::WHITE) }
  end

  def set_cell(x : Int32, y : Int32, char : Char, fg : TUI::Color, bg : TUI::Color, bold : Bool) : Nil
    raise "control character #{char.inspect} at #{x},#{y}" if char.control?
    # Like a terminal, overwriting half of a wide character erases the other half
    @cells[y][x - 1] = ' ' if @cells[y][x] == WIDE_RIGHT
    @cells[y][x + 1] = ' ' if x + 1 < @width && @cells[y][x + 1] == WIDE_RIGHT
    @cells[y][x] = char
    @colors[y][x] = fg
    if TUI::Text.width(char) == 2
      raise "wide character in the last column" if x + 1 >= @width
      @cells[y][x + 1] = WIDE_RIGHT
    end
  end

  def render : Nil
  end

  def sync : Nil
  end

  def close : Nil
    @closed = true
  end

  def text : String
    @cells.join("\n", &.reject(WIDE_RIGHT).join)
  end

  # Column where *text* starts on the screen, counting wide characters as two
  def column_of(text : String) : Int32?
    @cells.each do |row|
      line = row.map { |c| c == WIDE_RIGHT ? "" : c.to_s }
      line.each_index do |x|
        return x if line[x..].join.starts_with?(text)
      end
    end
  end

  # Color of the first character of *text* on the screen
  def color_of(text : String) : TUI::Color?
    @cells.each_with_index do |row, y|
      line = row.map { |c| c == WIDE_RIGHT ? "" : c.to_s }
      line.each_index do |x|
        return @colors[y][x] if line[x..].join.starts_with?(text)
      end
    end
  end

  # Row of the highest braille or block graph cell drawn in *color*
  def top_row(color : TUI::Color) : Int32?
    @cells.each_with_index do |row, y|
      row.each_with_index do |c, x|
        return y if (('⠁'..'⣿').includes?(c) || ('▁'..'█').includes?(c)) && @colors[y][x] == color
      end
    end
  end
end

private def tui_key(char : Char) : TUI::KeyEvent
  TUI::KeyEvent.char(char)
end

private def tui_key(key : TUI::Key) : TUI::KeyEvent
  TUI::KeyEvent.new(key)
end

private def tui_key(event : TUI::ResizeEvent) : TUI::ResizeEvent
  event
end

# Presses a key that maps to nothing every 10ms, then quits
class KeyRepeatTUIScreen < FakeTUIScreen
  def initialize(@presses : Int32)
    super()
  end

  def poll_event(timeout : Time::Span) : TUI::Event?
    return if timeout.zero?
    sleep 10.milliseconds
    @presses -= 1
    TUI::KeyEvent.char(@presses > 0 ? 'x' : 'q')
  end
end

# Calls *restart* before the first event
class RestartTUIScreen < FakeTUIScreen
  def initialize(events : Array(TUI::Event), @restart : -> Nil)
    super(events: events)
  end

  def poll_event(timeout : Time::Span) : TUI::Event?
    if restart = @restart
      @restart = nil
      restart.call
    end
    super
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
    recv_oct_details: {rate: 2048},
    send_oct_details: {rate: 4096},
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
        vhost:             "seed",
        user:              "guest",
        state:             "running",
        channels:          2,
        recv_oct_details:  {rate: 4096},
        send_oct_details:  {rate: 8192},
        client_properties: {connection_name: "seed-app"},
        name:              "127.0.0.1:50000 -> 127.0.0.1:5672",
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
      tracing:                 false,
    },
  ].to_json,
  "/api/nodes" => [
    {
      name:         "lavinmq@spec",
      uptime:       7_200_000,
      mem_used:     42_000_000,
      disk_free:    8_500_000_000,
      disk_total:   10_000_000_000,
      fd_used:      18,
      sockets_used: 4,
      run_queue:    0,
      followers:    [{remote_address: "10.0.0.2:5679", lag_in_bytes: 3072, id: "f1"}],
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
        component: "operator-target",
        vhost:     "seed",
        name:      "seed-target",
        value:     {
          "target": "amqp://guest:s3cret@localhost:5672/seed",
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
      error:         "failed to connect to amqp://guest:s3cret@localhost:1",
      message_count: 42,
    },
  ].to_json,
  "/api/federation-links" => [
    {
      vhost:     "seed",
      upstream:  "seed-upstream",
      type:      "exchange",
      resource:  "seed.topic",
      status:    "running",
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

# Yields a client and the requested resources (path and query)
# Requests other than GET are answered with *action_status* and recorded
# with their method
private def with_tui_api(status = 200, responses = TUI_RESPONSES, action_status = 204, &)
  requests = [] of String
  server = HTTP::Server.new do |context|
    unless context.request.method == "GET"
      requests << "#{context.request.method} #{context.request.resource}"
      context.response.status_code = action_status
      context.response.print({error: "forbidden", reason: "Access refused"}.to_json) if action_status >= 400
      next
    end
    requests << context.request.resource
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

private def run_tui(*keys, responses = TUI_RESPONSES, width = 140, height = 36, manage = false, action_status = 204) : {FakeTUIScreen, Array(String)}
  screen = FakeTUIScreen.new(width, height, keys.map { |k| tui_key(k).as(TUI::Event) }.to_a + [tui_key('q').as(TUI::Event)])
  requests = [] of String
  with_tui_api(responses: responses, action_status: action_status) do |client, reqs|
    TUI.new(client, 60.0, screen, manage: manage).start
    requests = reqs
  end
  {screen, requests}
end

private def with_response(path : String, body) : Hash(String, String)
  TUI_RESPONSES.merge({path => body.to_json})
end

private CHANNEL_NAME    = "127.0.0.1:50000 (1)"
private CONNECTION_NAME = "127.0.0.1:50000 -> 127.0.0.1:5672"

# The objects the rows of TUI_RESPONSES refer to
private VIEW_RESPONSES = TUI_RESPONSES.merge({
  "/api/queues/seed/seed.ready" => {
    vhost: "seed", name: "seed.ready", state: "running", consumers: 1,
    messages: 120, messages_ready: 118, messages_unacknowledged: 2,
    message_stats: {
      publish_details:     {rate: 8.2, log: [1.0, 2.0, 8.2]},
      deliver_get_details: {rate: 4.0, log: [1.0, 2.0, 4.0]},
    },
    messages_ready_log: [100, 110, 118], messages_unacknowledged_log: [1, 2, 2],
    consumer_details: [{
      consumer_tag: "seed-consumer-0", ack_required: true, prefetch_count: 25,
      queue: {vhost: "seed", name: "seed.ready"},
      channel_details: {name: CHANNEL_NAME, connection_name: CONNECTION_NAME, number: 1},
    }],
  }.to_json,
  "/api/queues/seed/seed.ready/bindings" => {
    items: [{source: "seed.direct", routing_key: "seed.key", arguments: {} of String => String}], filtered_count: 1,
  }.to_json,
  "/api/queues/seed/seed.ready/unacked" => {
    items: [{delivery_tag: 4242, consumer_tag: "seed-consumer-0", unacked_for_seconds: 75, channel_name: CHANNEL_NAME}], filtered_count: 1,
  }.to_json,
  "/api/channels/#{URI.encode_path_segment(CHANNEL_NAME)}" => {
    name: CHANNEL_NAME, vhost: "seed", user: "guest", state: "running",
    prefetch_count: 25, consumer_count: 1, messages_unacknowledged: 25,
    consumer_details: [{consumer_tag: "seed-consumer-0", ack_required: true, prefetch_count: 25, queue: {vhost: "seed", name: "seed.ready"}}],
  }.to_json,
  "/api/connections/#{URI.encode_path_segment(CONNECTION_NAME)}" => {
    name: CONNECTION_NAME, vhost: "seed", user: "guest", state: "running", channels: 2,
    client_properties: {connection_name: "seed-app"},
  }.to_json,
  "/api/connections/#{URI.encode_path_segment(CONNECTION_NAME)}/channels" => {
    items: [{name: CHANNEL_NAME, state: "running", messages_unacknowledged: 25, prefetch_count: 25, consumer_count: 1}], filtered_count: 2,
  }.to_json,
})

describe LavinMQCtl::TUI do
  {
    {'1', "Overview", ["Totals", "Disk used", "1.4GiB", "in 2.0KiB/s out 4.0KiB/s", "1 follower, lag 3.0KiB"]},
    {'2', "Queues", ["seed.ready", "1-1 of 25", "Msgs ↓"]},
    {'3', "Connections", ["127.0.0.1:50000", "8.0KiB/s", "seed-app"]},
    {'4', "Channels", ["Unacked"]},
    {'5', "Exchanges", ["seed.direct"]},
    {'6', "Consumers", ["seed-consumer-0"]},
    {'7', "Vhosts", ["seed"]},
    {'8', "Nodes", ["7.9GiB", "leader", "10.0.0.2:5679", "follower", "3.0KiB"]}, # disk_free is above Int32::MAX
    {'9', "Parameters", ["seed-shovel", "amqp://guest:***@localhost:5672/seed"]},
    {'0', "Policies", ["seed-ttl-dlx"]},
    {'s', "Shovels", ["seed-shovel", "amqp://guest:***@localhost:1"]},
    {'f', "Federation", ["seed-upstream", "running", "amqp://guest:***@localhost:5672/seed"]},
    {'u', "Users", ["administrator"]},
  }.each do |key, page, expected_texts|
    it "renders the #{page} page" do
      screen, _ = run_tui(key)

      screen.text.should contain(page)
      expected_texts.each { |text| screen.text.should contain(text) }
      screen.text.should_not contain("s3cret")
      screen.closed?.should be_true
    end
  end

  it "shows message counts as numbers, not bytes" do
    screen, _ = run_tui
    screen.text.should_not contain("3.8KiB")
  end

  it "draws both series of a graph on the same scale" do
    screen, _ = run_tui

    # Publish peaks at 12.5/s and deliver at 9.7/s, so publish reaches higher
    publish_top = screen.top_row(TUI::GREEN).should_not be_nil
    deliver_top = screen.top_row(TUI::BLUE).should_not be_nil
    publish_top.should be < deliver_top
  end

  it "shows both series of a graph when they are equal" do
    rate = {rate: 5.0, log: [5.0, 5.0, 5.0]}
    overview = JSON.parse(TUI_RESPONSES["/api/overview"]).as_h
    overview["message_stats"] = JSON.parse({publish_details: rate, deliver_get_details: rate}.to_json)
    screen, _ = run_tui(responses: with_response("/api/overview", overview))

    legend_row = screen.text.lines.index!(&.includes?("Publish"))
    publish_top = screen.top_row(TUI::GREEN).should_not be_nil
    deliver_top = screen.top_row(TUI::BLUE).should_not be_nil
    publish_top.should be < legend_row
    deliver_top.should be < legend_row
  end

  it "labels the graphs with how far back they go" do
    # Six samples, 5s apart by default
    screen, _ = run_tui
    screen.text.should contain("Message rates  last 25 s")
    screen.text.should contain("Queued messages  last 25 s")

    overview = JSON.parse(TUI_RESPONSES["/api/overview"]).as_h
    overview["stats_interval"] = JSON::Any.new(60_000_i64)
    screen, _ = run_tui(responses: with_response("/api/overview", overview))
    screen.text.should contain("Message rates  last 5 min")
  end

  it "fits the overview panels in a small terminal" do
    screen, _ = run_tui(width: 90, height: 24)

    screen.text.lines.select(&.includes?('╰')).each(&.should_not(match(/\w/)))
    screen.text.should contain("max 12.5/s")
  end

  it "asks for a bigger terminal below the smallest size" do
    screen, _ = run_tui('2', width: 39, height: 20)
    screen.text.should contain("Terminal too small")
    screen.text.should contain("39x20, needs 40x10")

    screen, _ = run_tui('2', TUI::ResizeEvent.new(40, 10))
    screen.text.should_not contain("Terminal too small")
    screen.text.should contain("Queues")
  end

  it "keeps the help between the header and the footer" do
    screen, _ = run_tui('?', width: 80, height: 12)
    lines = screen.text.lines
    lines.first.should contain("LAVINMQ")
    lines.last.should contain("? q")
    lines.count(&.includes?("Keys")).should eq 1
  end

  it "never writes control characters from the API to the terminal" do
    tag = "t\e]0;X\a\e[2J\u009b\r\u202E!"
    consumers = {items: [{consumer_tag: tag, queue: {vhost: "v\e[31m", name: "q\x7f"}}]}
    screen, _ = run_tui('6', responses: with_response("/api/consumers", consumers))

    # FakeTUIScreen raises on control characters
    screen.text.should contain("t?]0;X??[2J??!")
  end

  it "aligns columns after wide and zero width characters" do
    users = [
      {name: "队列日本語", tags: "wide"},
      {name: "é-​-ok", tags: "zero"},
      {name: "plain", tags: "plain"},
    ]
    screen, _ = run_tui('u', responses: with_response("/api/users", users))

    screen.text.should contain("队列日本語")
    screen.text.should contain("e--ok") # combining marks are left out
    plain = screen.column_of("plain    ").should_not be_nil
    screen.column_of("wide").should eq(plain + 25)
    screen.column_of("zero").should eq(plain + 25)
  end

  it "cuts wide characters at the column edge" do
    users = [{name: "队" * 30, tags: "after"}]
    screen, _ = run_tui('u', responses: with_response("/api/users", users))

    screen.text.should contain("#{"队" * 11}..")
    screen.text.should contain("after")
  end

  it "right-aligns numbers with thousands separators under their titles" do
    queues = {items: [
      {vhost: "v", name: "big", state: "running", messages: 12_345_678, messages_ready: 12_345_000, messages_unacknowledged: 678},
      {vhost: "v", name: "small", state: "running", messages: 7, messages_ready: 5, messages_unacknowledged: 2},
    ], filtered_count: 2}
    screen, _ = run_tui('2', responses: with_response("/api/queues", queues))

    lines = screen.text.lines
    ready_end = lines.find!(&.includes?("Ready")).index!("Ready") + "Ready".size
    big = lines.find!(&.includes?(" big "))
    big.index!("12,345,000").should eq(ready_end - "12,345,000".size)
    small = lines.find!(&.includes?(" small "))
    small[ready_end - 2, 3].should eq " 5 "
  end

  it "shortens numbers that don't fit and leaves out number columns that don't fit" do
    queue = {
      vhost: "v", name: "fast", state: "running", messages: 1,
      message_stats: {publish_details: {rate: 123_456_789.0}, deliver_get_details: {rate: 2.0}},
    }
    responses = with_response("/api/queues", {items: [queue], filtered_count: 1})
    screen, _ = run_tui('2', responses: responses)
    screen.text.should contain(" 123M ")
    screen.text.should_not contain("123,4")

    # Cut off, 123M would read as 12 or 1
    screen, _ = run_tui('2', responses: responses, width: 90)
    screen.text.should contain("Cons")
    screen.text.should_not contain("Pub")
    screen.text.should_not contain("123")
  end

  it "lines up the overview's message total and rates on their decimal points" do
    screen, _ = run_tui

    lines = screen.text.lines
    total = lines.compact_map(&.match(/│  Total +([\d,]+)/)).first
    publish = lines.compact_map(&.match(/│  Publish +([\d,]+)\.\d\/s/)).first
    total[1].should eq "4,200"
    publish.end(1).should eq total.end(1)
  end

  it "survives values that don't fit the expected types" do
    random = Random.new(42)
    values = [
      nil, true, -1, 0, 1e300, -1e300, 9_223_372_036_854_775_807, 1.5, "",
      "\e[2J\u{1F680}队", [1, "a"], {"a" => 1},
    ] of JSON::Any::Type | Int32 | Array(Int32 | String) | Hash(String, Int32)
    keys = %w[name vhost messages messages_ready uptime mem_used mem_limit disk_free disk_total fd_used fd_total
      rate log items filtered_count queue_totals object_totals message_stats publish_details deliver_get_details
      followers lag_in_bytes recv_oct_details send_oct_details value consumer_tag queue tags definition error uri]
    random_json = uninitialized Proc(Int32, JSON::Any)
    random_json = ->(depth : Int32) do
      if depth > 0 && random.next_bool
        JSON::Any.new(keys.sample(4, random).to_h { |k| {k, random_json.call(depth - 1)} })
      elsif depth > 0 && random.rand(4) == 0
        JSON::Any.new(Array.new(3) { random_json.call(depth - 1) })
      else
        JSON.parse(values.sample(random).to_json)
      end
    end
    20.times do
      responses = TUI_RESPONSES.transform_values { random_json.call(4).to_json }
      screen, _ = run_tui('2', TUI::Key::Enter, '3', TUI::Key::Enter, '4', TUI::Key::Enter, '5', TUI::Key::Enter,
        '6', TUI::Key::Enter, '7', TUI::Key::Enter, '8', TUI::Key::Enter, '9', TUI::Key::Enter, '0', TUI::Key::Enter,
        's', TUI::Key::Enter, 'f', TUI::Key::Enter, 'u', TUI::Key::Enter, 'l', TUI::Key::Home, '1', responses: responses)
      screen.closed?.should be_true
    end
  end

  it "shows every field of the selected row" do
    queue = {
      vhost: "seed", name: "seed.stream", state: "running", messages: 5,
      arguments: {"x-queue-type": "stream"},
      message_stats: {publish: 10, publish_details: {rate: 2.5}},
      total_bytes: 3 * 1024 * 1024, ready_avg_bytes: 512, message_bytes_ready: 2048,
      recv_oct: 4096, recv_oct_details: {rate: 2048},
      error: "x" * 300,
    }
    responses = TUI_RESPONSES.merge({
      "/api/queues"                  => {items: [queue], filtered_count: 1}.to_json,
      "/api/queues/seed/seed.stream" => queue.to_json,
    })
    # The last section
    screen, _ = run_tui('2', TUI::Key::Enter, TUI::Key::Left, responses: responses, height: 50)

    screen.text.should contain("Queues › seed.stream")
    screen.text.should match(/arguments\.x-queue-type +stream/)
    screen.text.should match(/message_stats\.publish +10 \(2\.5\/s\)/)
    screen.text.should_not contain("publish_details")
    screen.text.should match(/total_bytes +3145728 \(3\.0MiB\)/)
    screen.text.should match(/ready_avg_bytes +512 /)
    screen.text.should match(/message_bytes_ready +2048 \(2\.0KiB\)/)
    screen.text.should match(/recv_oct +4096 \(4\.0KiB\) \(2\.0KiB\/s\)/)
    screen.text.should contain("x" * 100) # long values wrap
  end

  it "hides password hashes and URI credentials in the details" do
    users = [{name: "guest", password_hash: "c2VjcmV0aGFzaA=="}]
    screen, _ = run_tui('u', TUI::Key::Enter, responses: with_response("/api/users", users))
    screen.text.should match(/password_hash +\(hidden\)/)
    screen.text.should_not contain("c2VjcmV0aGFzaA")

    screen, _ = run_tui('f', TUI::Key::Enter)
    screen.text.should contain("Federation links › seed-upstream")
    screen.text.should match(%r{uri +amqp://guest:\*\*\*@localhost})
    screen.text.should_not contain("s3cret")

    parameters = JSON.parse(TUI_RESPONSES["/api/parameters"]).as_h.merge({"filtered_count" => JSON::Any.new(2)})
    screen, _ = run_tui('9', TUI::Key::Down, TUI::Key::Enter, responses: with_response("/api/parameters", parameters))
    screen.text.should match(%r{value\.target +amqp://guest:\*\*\*@localhost})
    screen.text.should_not contain("s3cret")
  end

  it "scrolls the details and goes back to the table" do
    exchange = JSON.parse((1..60).to_h { |i| {"field#{i}", i} }.to_json)
    responses = with_response("/api/exchanges", {items: [exchange], filtered_count: 1})
    # End and a key after it before the screen is drawn again
    screen, _ = run_tui('5', TUI::Key::Enter, TUI::Key::End, TUI::Key::Down, responses: responses)
    screen.text.should contain("lines 33-60 of 60")
    screen.text.should contain("field60")
    screen.text.should_not contain("field1 ")

    screen, _ = run_tui('5', TUI::Key::Enter, 'j', TUI::Key::Escape, responses: responses)
    screen.text.should contain("Exchanges  1  1-1 of 1")
  end

  it "opens a queue with its consumers, bindings and unacked messages" do
    screen, requests = run_tui('2', TUI::Key::Enter, responses: VIEW_RESPONSES)
    screen.text.should contain("Queues › seed.ready")
    screen.text.should contain("Consumers 1")
    screen.text.should contain("seed-consumer-0")
    screen.text.should contain("Message rates  last 10 s")
    screen.text.should contain("Queued messages  last 10 s")
    requests.any?(&.starts_with?("/api/queues/seed/seed.ready?consumer_list_length=")).should be_true

    screen, requests = run_tui('2', TUI::Key::Enter, TUI::Key::Tab, responses: VIEW_RESPONSES)
    screen.text.should match(/seed\.direct +seed\.key/)
    requests.any?(&.starts_with?("/api/queues/seed/seed.ready/bindings?page=1&")).should be_true

    screen, requests = run_tui('2', TUI::Key::Enter, TUI::Key::Tab, TUI::Key::Tab, responses: VIEW_RESPONSES)
    screen.text.should match(/4,242 +1m15s +seed-consumer-0/)
    requests.any?(&.matches?(%r{^/api/queues/seed/seed\.ready/unacked\?page=1&.*sort=unacked_for_seconds&sort_reverse=true})).should be_true
  end

  it "opens what a row refers to and goes back" do
    screen, requests = run_tui('2', TUI::Key::Enter, TUI::Key::Enter, responses: VIEW_RESPONSES)
    screen.text.should contain("Queues › seed.ready › #{CHANNEL_NAME}")
    requests.should contain("/api/channels/#{URI.encode_path_segment(CHANNEL_NAME)}")
    screen.text.should contain("At its prefetch limit")

    screen, _ = run_tui('2', TUI::Key::Enter, TUI::Key::Enter, TUI::Key::Escape, responses: VIEW_RESPONSES)
    screen.text.lines.find!(&.includes?("Queues ›")).should_not contain(CHANNEL_NAME)
    screen.text.should contain("seed-consumer-0")

    screen, _ = run_tui('2', TUI::Key::Enter, TUI::Key::Enter, TUI::Key::Escape, TUI::Key::Escape, responses: VIEW_RESPONSES)
    screen.text.should contain("Queues  25  1-1 of 25")
  end

  it "opens a connection with its channels" do
    screen, _ = run_tui('3', TUI::Key::Enter, responses: VIEW_RESPONSES)
    screen.text.should contain("Connections › #{CONNECTION_NAME}")
    screen.text.should contain("Channels 2")
    screen.text.should match(/#{Regex.escape(CHANNEL_NAME)} +● running +25 +25 +1/)
  end

  it "opens the channel of a consumer" do
    screen, _ = run_tui('6', TUI::Key::Enter, responses: VIEW_RESPONSES)
    screen.text.should contain("Consumers › #{CHANNEL_NAME}")
  end

  it "highlights what needs attention" do
    queues = {items: [
      {vhost: "v", name: "busy", messages_ready: 7_777, consumers: 1},
      {vhost: "v", name: "idle", messages_ready: 5_555, consumers: 0},
      {vhost: "v", name: "empty", messages_ready: 0, consumers: 0},
    ], filtered_count: 3}
    screen, _ = run_tui('2', responses: with_response("/api/queues", queues))
    screen.color_of("7,777").should eq TUI::WHITE # selected
    screen.color_of("5,555").should eq TUI::YELLOW
    screen.color_of("0  ").should_not eq TUI::YELLOW

    channels = {items: [
      {name: "full", prefetch_count: 10, consumer_count: 2, messages_unacknowledged: 20},
      {name: "room", prefetch_count: 10, consumer_count: 2, messages_unacknowledged: 19},
    ], filtered_count: 2}
    screen, _ = run_tui('4', responses: with_response("/api/channels", channels))
    screen.color_of("20 ").should eq TUI::YELLOW
    screen.color_of("19 ").should eq TUI::TEXT_FG

    shovels = [{vhost: "v", name: "broken", state: "terminated", error: "Connection refused"}]
    screen, _ = run_tui('s', responses: with_response("/api/shovels", shovels))
    screen.color_of("Connection refused").should eq TUI::RED
  end

  it "shows the broker's log, newest last" do
    log = [
      "2026-10-10 23:43:01 UTC [INFO] lmq.launcher - Starting LavinMQ",
      "2026-10-10 23:43:08 UTC [WARN] lmq.launcher - sysctl -w vm.max_map_count=1000000",
      "2026-10-10 23:43:26 UTC [ERROR] lmq.amqp.client - Read timed out",
      "with a second line",
    ].join('\n')
    screen, requests = run_tui('l', responses: TUI_RESPONSES.merge({"/api/logs" => log}))
    requests.should contain("/api/logs")
    screen.text.should contain("Logs  4")
    screen.text.should match(/10-10 23:43:08 WARN +lmq\.launcher sysctl -w/)
    screen.color_of("WARN").should eq TUI::YELLOW
    screen.color_of("ERROR").should eq TUI::RED
    lines = screen.text.lines
    lines.index!(&.includes?("Read timed out")).should be < lines.index!(&.includes?("with a second line"))

    screen, _ = run_tui('l', '/', 'w', 'a', 'r', 'n', TUI::Key::Enter, responses: TUI_RESPONSES.merge({"/api/logs" => log}))
    screen.text.should contain("Logs  1")
    screen.text.should_not contain("Starting LavinMQ")
  end

  it "scrolls the log and follows new entries at the end" do
    log = (1..100).join('\n') { |i| "2026-10-10 23:43:08 UTC [INFO] lmq.spec - entry #{i}." }
    responses = TUI_RESPONSES.merge({"/api/logs" => log})
    screen, _ = run_tui('l', responses: responses)
    screen.text.should contain("entry 100.")
    screen.text.should_not contain("entry 1.")

    screen, _ = run_tui('l', TUI::Key::Home, responses: responses)
    screen.text.should contain("entry 1.")
    screen.text.should_not contain("entry 100.")

    screen, _ = run_tui('l', TUI::Key::Home, TUI::Key::End, responses: responses)
    screen.text.should contain("entry 100.")
  end

  it "says why the log is empty" do
    screen, _ = run_tui('l')
    screen.text.should contain("only shown to users with the administrator tag")
  end

  it "changes nothing unless started with --manage" do
    screen, requests = run_tui('2', 'm', '1', 'y')
    screen.text.should contain("Read-only: start lavinmqctl tui with --manage")
    screen.text.should_not contain("MANAGE")
    requests.none?(&.starts_with?("PUT")).should be_true
  end

  it "pauses a queue after confirming" do
    # The menu and the question take any key but Ctrl-C
    screen, _ = run_tui('2', 'm', TUI::Key::CtrlC, manage: true)
    screen.text.should contain(" MANAGE ")
    screen.text.should contain("1  Pause the consumers of queue seed.ready")

    screen, _ = run_tui('2', 'm', '1', TUI::Key::CtrlC, manage: true)
    screen.text.should contain("Pause the consumers of queue seed.ready in vhost seed?")

    screen, requests = run_tui('2', 'm', '1', 'y', manage: true)
    requests.should contain("PUT /api/queues/seed/seed.ready/pause")
    screen.text.should contain("Paused the consumers of queue seed.ready in vhost seed")

    screen, requests = run_tui('2', 'm', '1', 'n', manage: true)
    requests.none?(&.starts_with?("PUT")).should be_true
    screen.text.should contain("Cancelled, nothing was changed")

    _, requests = run_tui('2', 'm', TUI::Key::Escape, 'y', manage: true)
    requests.none?(&.starts_with?("PUT")).should be_true
  end

  it "says why a change failed" do
    screen, _ = run_tui('2', 'm', '1', 'y', manage: true, action_status: 403)
    screen.text.should contain("Failed: HTTP 403 Access refused")
  end

  it "closes connections and channels and cancels consumers" do
    _, requests = run_tui('3', 'm', '1', 'y', responses: VIEW_RESPONSES, manage: true)
    requests.should contain("DELETE /api/connections/#{URI.encode_path_segment(CONNECTION_NAME)}")

    _, requests = run_tui('4', 'm', '1', 'y', responses: VIEW_RESPONSES, manage: true)
    requests.should contain("DELETE /api/channels/#{URI.encode_path_segment(CHANNEL_NAME)}")

    # The queue of the view, and the consumer selected in it
    screen, _ = run_tui('2', TUI::Key::Enter, 'm', TUI::Key::CtrlC, responses: VIEW_RESPONSES, manage: true)
    screen.text.should contain("1  Pause the consumers of queue seed.ready")
    screen.text.should contain("2  Cancel consumer seed-consumer-0")
    _, requests = run_tui('2', TUI::Key::Enter, 'm', '2', 'y', responses: VIEW_RESPONSES, manage: true)
    requests.should contain("DELETE /api/consumers/seed/#{URI.encode_path_segment(CONNECTION_NAME)}/1/seed-consumer-0")
  end

  it "warns about a queue without consumers, and an object that's gone" do
    queue = {vhost: "seed", name: "seed.ready", messages_ready: 5, consumers: 0}
    screen, _ = run_tui('2', TUI::Key::Enter, responses: with_response("/api/queues/seed/seed.ready", queue))
    screen.text.should contain("No consumers: 5 messages are waiting")

    screen, _ = run_tui('2', TUI::Key::Enter)
    screen.text.should contain("Not found anymore")
    screen.text.should contain("seed.ready")
  end

  it "fetches the next page of rows from the API" do
    queues = JSON.parse(TUI_RESPONSES["/api/queues"]).as_h.merge({"filtered_count" => JSON::Any.new(100)})
    _, requests = run_tui('2', TUI::Key::PageDown, responses: with_response("/api/queues", queues))

    requests.should contain("/api/queues?page=2&page_size=28&sort=messages&sort_reverse=true")
  end

  it "doesn't fetch again when moving within the fetched rows" do
    _, requests = run_tui('2', TUI::Key::Down, TUI::Key::Down, TUI::Key::Up)

    requests.count(&.starts_with?("/api/queues?page=1&page_size=28")).should eq 1
  end

  it "filters by name and sorts by another column" do
    _, requests = run_tui('2', '/', 'a', 'b', TUI::Key::Backspace, 'c', TUI::Key::Enter, 'o', 'r')

    requests.should contain("/api/queues?page=1&page_size=28&sort=messages&sort_reverse=true&name=ac")
    requests.should contain("/api/queues?page=1&page_size=28&sort=messages_ready&sort_reverse=true&name=ac")
    requests.should contain("/api/queues?page=1&page_size=28&sort=messages_ready&sort_reverse=false&name=ac")
  end

  it "backs off when the broker is slow to answer" do
    TUI.refresh_delay(1.second, 20.milliseconds).should eq 1.second
    TUI.refresh_delay(1.second, 300.milliseconds).should eq 3.seconds
    TUI.refresh_delay(1.second, 10.seconds).should eq 30.seconds
  end

  {"TCP", "the control socket"}.each do |transport|
    it "recovers from a timed out request over #{transport}" do
      stalled = false
      server = HTTP::Server.new do |context|
        context.response.content_type = "application/json"
        if context.request.path == "/api/overview" && !stalled
          stalled = true
          sleep 300.milliseconds
          begin
            context.response.print({lavinmq_version: "stale"}.to_json)
            context.response.close
          rescue HTTP::Server::ClientError | IO::Error
            # The TUI has closed the connection by now
          end
        else
          context.response.print TUI_RESPONSES[context.request.path]? || "{}"
        end
      end
      if transport == "TCP"
        addr = server.bind_tcp("127.0.0.1", 0)
        client = HTTP::Client.new("127.0.0.1", addr.port)
        client.read_timeout = 200.milliseconds
      else
        path = File.tempname("lavinmqctl-tui", ".sock")
        server.bind_unix(path)
        connect = -> {
          socket = UNIXSocket.new(path)
          socket.read_timeout = 200.milliseconds
          HTTP::Client.new(socket)
        }
        client = connect.call
      end
      spawn(name: "tui spec api") { server.listen }

      # The overview times out on the first refresh, the queues page fetches it again
      screen = FakeTUIScreen.new(events: [tui_key('2'), tui_key('q')] of TUI::Event)
      TUI.new(client, 60.0, screen, reconnect: connect).start

      # The late response to the timed out request isn't read as the answer to a
      # later one, and the next refresh gets through
      screen.text.should contain("vspec")
    ensure
      client.try &.close
      server.try &.close
    end
  end

  it "reconnects on the control socket after the connection is lost" do
    path = File.tempname("lavinmqctl-tui", ".sock")
    version = "before"
    server = HTTP::Server.new do |context|
      context.response.content_type = "application/json"
      context.response.print(context.request.path == "/api/overview" ? {lavinmq_version: version}.to_json : "[]")
    end
    server.bind_unix(path)
    spawn(name: "tui spec api") { server.listen }
    sockets = [] of UNIXSocket
    connect = -> {
      socket = UNIXSocket.new(path)
      socket.read_timeout = 1.second
      sockets << socket
      HTTP::Client.new(socket)
    }
    client = connect.call
    # Drops the connection before the first key, as when the broker restarts
    events = [tui_key('2'), tui_key('3'), tui_key('1'), tui_key('q')] of TUI::Event
    screen = RestartTUIScreen.new(events, -> {
      sockets.each(&.close)
      version = "after"
      nil
    })
    TUI.new(client, 60.0, screen, reconnect: connect).start

    screen.text.should contain("vafter")
  ensure
    sockets.try &.each(&.close)
    server.try &.close
  end

  it "reconnects on the control socket when the server closes the connection" do
    path = File.tempname("lavinmqctl-tui", ".sock")
    server = HTTP::Server.new do |context|
      # As when the broker restarts between two requests
      context.response.headers["Connection"] = "close"
      context.response.content_type = "application/json"
      context.response.print(context.request.path == "/api/overview" ? {lavinmq_version: "spec"}.to_json : "[]")
    end
    server.bind_unix(path)
    spawn(name: "tui spec api") { server.listen }
    connect = -> { HTTP::Client.new(UNIXSocket.new(path)) }
    screen = FakeTUIScreen.new(events: [tui_key('2'), tui_key('1'), tui_key('q')] of TUI::Event)
    TUI.new(connect.call, 60.0, screen, reconnect: connect).start

    screen.text.should contain("vspec")
    screen.text.should_not contain("reconnected")
  ensure
    server.try &.close
  end

  it "redraws right away at a new size and fetches for it once the size settles" do
    resize = TUI::ResizeEvent.new(100, 20)
    # Quits before the size settles
    screen, requests = run_tui('2', resize)
    screen.text.lines.size.should eq 20
    screen.text.lines.first.size.should eq 100
    screen.text.should contain("seed.ready")
    requests.none?(&.includes?("page_size=12")).should be_true

    screen = FakeTUIScreen.new(events: [tui_key('2'), resize] of TUI::Event, quit_after: 1.second)
    with_tui_api do |client, reqs|
      TUI.new(client, 60.0, screen).start
      reqs.should contain("/api/queues?page=1&page_size=12&sort=messages&sort_reverse=true")
    end
  end

  it "keeps the selected row through a resize until the rows are fetched again" do
    items = (0...30).map { |i| {vhost: "v", name: "q%02d" % i} }
    queues = {items: items, filtered_count: 60}
    screen, _ = run_tui('2', TUI::Key::PageDown, TUI::ResizeEvent.new(140, 30), responses: with_response("/api/queues", queues))
    screen.text.lines.find!(&.includes?("▌")).should contain("q00")
  end

  it "keeps the selection on its row when the order changes" do
    queue = ->(name : String) { {vhost: "v", name: name, messages: 1} }
    responses = TUI_RESPONSES.merge({"/api/queues" => {items: %w[a.queue b.queue c.queue].map(&queue), filtered_count: 3}.to_json})
    with_tui_api(responses: responses) do |client|
      screen = FakeTUIScreen.new(events: [tui_key('2'), tui_key(TUI::Key::Down)] of TUI::Event, quit_after: 500.milliseconds)
      spawn do
        sleep 200.milliseconds
        responses["/api/queues"] = {items: %w[b.queue c.queue a.queue].map(&queue), filtered_count: 3}.to_json
      end
      TUI.new(client, 0.05, screen).start

      screen.text.lines.find!(&.includes?("▌")).should contain("b.queue")
      screen.text.lines.index!(&.includes?("b.queue")).should be < screen.text.lines.index!(&.includes?("c.queue"))
    end
  end

  it "keeps refreshing while keys are pressed" do
    with_tui_api do |client, requests|
      screen = KeyRepeatTUIScreen.new(presses: 30)
      TUI.new(client, 0.05, screen).start

      requests.count("/api/overview").should be >= 3
    end
  end

  it "renders HTTP errors in the footer" do
    with_tui_api(status: 401) do |client|
      screen = FakeTUIScreen.new(events: [tui_key('q')] of TUI::Event)
      TUI.new(client, 60.0, screen).start

      screen.text.should contain("overview: HTTP 401 UNAUTHORIZED")
      screen.closed?.should be_true
    end
  end
end

private def key_names(events : Array(TUI::KeyEvent)) : Array(String)
  events.map { |e| e.key.char? ? e.char.to_s : e.key.to_s }
end

describe LavinMQCtl::TUI::KeyParser do
  it "parses characters, control keys and escape sequences" do
    events = TUI::KeyParser.new.parse("a\e[A\e[B\e[1;5C\eOD\e[5~\e[6~\eOH\e[4~\e[Z\r\x7f\t\x03é队".to_slice)
    key_names(events).should eq %w[a Up Down Right Left PageUp PageDown Home End BackTab Enter Backspace Tab CtrlC é 队]
  end

  it "waits for the rest of a split escape sequence or character" do
    parser = TUI::KeyParser.new
    parser.parse("\e[".to_slice).should be_empty
    key_names(parser.parse("A\xe9\x98".to_slice)).should eq %w[Up]
    key_names(parser.parse("\x9f".to_slice)).should eq %w[队]
  end

  it "takes a lone ESC as the Escape key once no more bytes come" do
    parser = TUI::KeyParser.new
    parser.parse("\e".to_slice).should be_empty
    key_names(parser.parse(Bytes.empty, flush: true)).should eq %w[Escape]
  end

  it "drops unknown and overlong sequences and invalid UTF-8" do
    parser = TUI::KeyParser.new
    key_names(parser.parse("\e[9;9X\xff\xfeb".to_slice)).should eq %w[b]
    parser.parse(("\e[" + "1;" * 40).to_slice).should be_empty
    key_names(parser.parse("1;2qb".to_slice)).should eq %w[b]
    parser.pending?.should be_false
  end

  it "never takes Escape or Ctrl-C as part of a broken sequence" do
    parser = TUI::KeyParser.new
    parser.parse(("\e[" + "9;" * 30).to_slice).should be_empty
    key_names(parser.parse("\e[A".to_slice)).should eq %w[Up]
    key_names(parser.parse("\e[1;\x03\xe9\x03\eO\x03".to_slice)).should eq %w[CtrlC CtrlC Escape O CtrlC]
    parser.parse(("\e[" + "9;" * 30).to_slice).should be_empty
    parser.pending?.should be_true
    parser.parse(Bytes.empty, flush: true).should be_empty
    key_names(parser.parse("1".to_slice)).should eq %w[1]
  end
end

describe LavinMQCtl::TUI::Renderer do
  it "only writes the cells that changed" do
    io = IO::Memory.new
    renderer = TUI::Renderer.new(io, 10, 2)
    renderer.set_cell(0, 0, 'a', TUI::WHITE, TUI::BG, false)
    renderer.render
    io.clear
    renderer.render
    io.to_s.should eq "\e[?2026h\e[?2026l"

    io.clear
    renderer.set_cell(5, 1, 'b', TUI::WHITE, TUI::BG, false)
    renderer.render
    io.to_s.should eq "\e[?2026h\e[2;6H\e[0;38;2;250;250;250;48;2;24;24;24mb\e[?2026l"
  end

  it "clears and redraws in the same synchronized update after a resize" do
    io = IO::Memory.new
    renderer = TUI::Renderer.new(io, 4, 1)
    renderer.set_cell(0, 0, 'a', TUI::WHITE, TUI::BG, false)
    renderer.render
    io.clear
    renderer.resize(3, 1)
    renderer.set_cell(0, 0, 'a', TUI::WHITE, TUI::BG, false)
    renderer.render
    io.to_s.should start_with("\e[?2026h\e[0m\e[2J")
    io.to_s.should contain("a")
    io.to_s.should end_with("\e[?2026l")
  end

  it "positions the cursor after characters terminals may count differently" do
    io = IO::Memory.new
    renderer = TUI::Renderer.new(io, 10, 1)
    renderer.set_cell(0, 0, '队', TUI::WHITE, TUI::BG, false)
    renderer.set_cell(2, 0, 'x', TUI::WHITE, TUI::BG, false)
    renderer.render
    io.to_s.should contain("队\e[1;3Hx")
  end

  it "never writes control characters" do
    io = IO::Memory.new
    renderer = TUI::Renderer.new(io, 4, 1)
    "\e]\u009b\a".each_char_with_index { |c, i| renderer.set_cell(i, 0, c, TUI::WHITE, TUI::BG, false) }
    renderer.render
    io.to_s.should contain("?]??")
    io.to_s.should_not contain("\e]")
  end

  it "uses the 256 color palette without truecolor" do
    TUI::Renderer.xterm256(TUI::Color.rgb(0, 0, 0)).should eq 16
    TUI::Renderer.xterm256(TUI::Color.rgb(255, 255, 255)).should eq 231
    TUI::Renderer.xterm256(TUI::Color.rgb(255, 0, 0)).should eq 196
    TUI::Renderer.xterm256(TUI::Color.rgb(128, 128, 128)).should eq 244
    TUI::Renderer.xterm256(TUI::Color.rgb(6, 10, 18)).should eq 232
  end
end
