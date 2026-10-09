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

  # Row of the highest braille graph cell drawn in *color*
  def top_row(color : TUI::Color) : Int32?
    @cells.each_with_index do |row, y|
      row.each_with_index do |c, x|
        return y if ('⠁'..'⣿').includes?(c) && @colors[y][x] == color
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
private def with_tui_api(status = 200, responses = TUI_RESPONSES, &)
  requests = [] of String
  server = HTTP::Server.new do |context|
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

private def run_tui(*keys, responses = TUI_RESPONSES, width = 140, height = 36) : {FakeTUIScreen, Array(String)}
  screen = FakeTUIScreen.new(width, height, keys.map { |k| tui_key(k).as(TUI::Event) }.to_a + [tui_key('q').as(TUI::Event)])
  requests = [] of String
  with_tui_api(responses: responses) do |client, reqs|
    TUI.new(client, 60.0, screen).start
    requests = reqs
  end
  {screen, requests}
end

private def with_response(path : String, body) : Hash(String, String)
  TUI_RESPONSES.merge({path => body.to_json})
end

describe LavinMQCtl::TUI do
  {
    {'1', "Overview", ["Object totals", "in 2.0KiB/s out 4.0KiB/s", "1 follower, lag 3.0KiB"]},
    {'2', "Queues", ["seed.ready", "1-1 of 25", "Msgs ▼"]},
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
    publish_top = screen.top_row(TUI::CYAN).should_not be_nil
    deliver_top = screen.top_row(TUI::MAGENTA).should_not be_nil
    publish_top.should be < deliver_top
  end

  it "shows both series of a graph when they are equal" do
    rate = {rate: 5.0, log: [5.0, 5.0, 5.0]}
    overview = JSON.parse(TUI_RESPONSES["/api/overview"]).as_h
    overview["message_stats"] = JSON.parse({publish_details: rate, deliver_get_details: rate}.to_json)
    screen, _ = run_tui(responses: with_response("/api/overview", overview))

    legend_row = screen.text.lines.index!(&.includes?("publish"))
    publish_top = screen.top_row(TUI::CYAN).should_not be_nil
    deliver_top = screen.top_row(TUI::MAGENTA).should_not be_nil
    publish_top.should be < legend_row
    deliver_top.should be < legend_row
  end

  it "fits the overview panels in a small terminal" do
    screen, _ = run_tui(width: 90, height: 24)

    screen.text.lines.select(&.includes?('╰')).each(&.should_not(match(/\w/)))
    screen.text.should contain("max 12.5/s")
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
      screen, _ = run_tui('2', '3', '4', '5', '6', '7', '8', '9', '0', 's', 'f', 'u', '1', responses: responses)
      screen.closed?.should be_true
    end
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
    io.to_s.should eq "\e[?2026h\e[2;6H\e[0;38;2;238;244;252;48;2;6;10;18mb\e[?2026l"
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
