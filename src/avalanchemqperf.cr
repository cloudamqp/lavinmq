require "./avalanchemq/version"
require "./stdlib/*"
require "option_parser"
require "amqp-client"
require "benchmark"
require "json"

class Perf
  @uri = "amqp://guest:guest@localhost"
  getter banner

  def initialize
    @parser = OptionParser.new
    @banner = "Usage: #{PROGRAM_NAME} [throughput | bind-churn | queue-churn | connection-churn] [arguments]"
    @parser.banner = @banner
    @parser.on("-h", "--help", "Show this help") { puts @parser; exit 1 }
    @parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
    @parser.on("--build-info", "Show build information") { puts AvalancheMQ::BUILD_INFO; exit 0 }
    @parser.invalid_option { |arg| abort "Invalid argument: #{arg}" }
    @parser.on("--uri=URI", "URI to connect to (default #{@uri})") do |v|
      @uri = v
    end
  end

  def run(args = ARGV)
    @parser.parse(args)
  end
end

class Throughput < Perf
  @publishers = 1
  @consumers = 1
  @size = 16
  @exchange = ""
  @queue = "perf-test"
  @routing_key = "perf-test"
  @ack = 0
  @rate = 0
  @consume_rate = 0
  @confirm = 0
  @persistent = false
  @prefetch = 0_u32
  @quiet = false
  @poll = false
  @json_output = false
  @timeout = Time::Span.zero
  @pmessages = 0
  @cmessages = 0
  @queue_args = AMQP::Client::Arguments.new
  @consumer_args = AMQP::Client::Arguments.new

  def initialize
    super
    @parser.on("-x publishers", "--publishers=number", "Number of publishers (default 1)") do |v|
      @publishers = v.to_i
    end
    @parser.on("-y consumers", "--consumers=number", "Number of consumers (default 1)") do |v|
      @consumers = v.to_i
    end
    @parser.on("-s msgsize", "--size=bytes", "Size of each message (default 16 bytes)") do |v|
      @size = v.to_i
    end
    @parser.on("-a MESSAGES", "--ack MESSAGES", "Ack after X consumed messages (default 0)") do |v|
      @ack = v.to_i
    end
    @parser.on("-c outstanding", "--confirm max-unconfirmed", "Confirm publishes every X messages") do |v|
      @confirm = v.to_i
    end
    @parser.on("-u queue", "--queue=name", "Queue name (default perf-test)") do |v|
      @queue = v
      @routing_key = v
    end
    @parser.on("-k routing-key", "--routing-key=name", "Routing key (default queue name)") do |v|
      @routing_key = v
    end
    @parser.on("-e exchange", "--exchange=name", "Exchange to publish to (default \"\")") do |v|
      @exchange = v
    end
    @parser.on("-r pub-rate", "--rate=number", "Max publish rate (default 0)") do |v|
      @rate = v.to_i
    end
    @parser.on("-R consumer-rate", "--consumer-rate=number", "Max consume rate (default 0)") do |v|
      @consume_rate = v.to_i
    end
    @parser.on("-p", "--persistent", "Persistent messages (default false)") do
      @persistent = true
    end
    @parser.on("-P", "--prefetch=number", "Number of messages to prefetch (default 0, unlimited)") do |v|
      @prefetch = v.to_u32
    end
    @parser.on("-g", "--poll", "Poll with basic_get instead of consuming") do
      @poll = true
    end
    @parser.on("-j", "--json", "Output result as JSON") do
      @json_output = true
    end
    @parser.on("-z seconds", "--time=seconds", "Only run for X seconds") do |v|
      @timeout = Time::Span.new(seconds: v.to_i)
    end
    @parser.on("-q", "--quiet", "Quiet, only print the summary") do
      @quiet = true
    end
    @parser.on("-C messages", "--pmessages=messages", "Publish max X number of messages") do |v|
      @pmessages = v.to_i
    end
    @parser.on("-D messages", "--cmessages=messages", "Consume max X number of messages") do |v|
      @cmessages = v.to_i
    end
    @parser.on("--queue-args=JSON", "Queue arguments as a JSON string") do |v|
      args = JSON.parse(v).as_h
      @queue_args = AMQP::Client::Arguments.new(args)
    end
    @parser.on("--consumer-args=JSON", "Consumer arguments as a JSON string") do |v|
      args = JSON.parse(v).as_h
      @consumer_args = AMQP::Client::Arguments.new(args)
    end
  end

  @pubs = 0_u64
  @consumes = 0_u64
  @stopped = false

  def run
    super

    done = Channel(Nil).new
    @consumers.times do
      if @poll
        spawn poll_consume(done)
      else
        spawn consume(done)
      end
    end

    @publishers.times do
      spawn pub(done)
    end

    if @timeout != Time::Span.zero
      spawn do
        sleep @timeout
        @stopped = true
      end
    end

    Fiber.yield # wait for all clients to connect
    start = Time.monotonic
    Signal::INT.trap do
      abort "Aborting" if @stopped
      @stopped = true
      summary(start, done)
      exit 0
    end

    spawn do
      (@publishers + @consumers).times { done.receive }
      @stopped = true
    end

    loop do
      break if @stopped
      pubs_last = @pubs
      consumes_last = @consumes
      sleep 1
      unless @quiet
        print "Publish rate: "
        print @pubs - pubs_last
        print " msgs/s Consume rate: "
        print @consumes - consumes_last
        print " msgs/s\n"
      end
    end
    summary(start, done)
  end

  private def summary(start : Time::Span, done)
    stop = Time.monotonic
    elapsed = (stop - start).total_seconds
    avg_pub = (@pubs / elapsed).round(1)
    avg_consume = (@consumes / elapsed).round(1)
    if @json_output
      print "\n"
      JSON.build(STDOUT) do |json|
        json.object do
          json.field "elapsed_seconds", elapsed
          json.field "avg_pub_rate", avg_pub
          json.field "avg_consume_rate", avg_consume
        end
      end
      print "\n"
    else
      print "\nSummary:\n"
      print "Average publish rate: "
      print avg_pub
      print " msgs/s\n"
      print "Average consume rate: "
      print avg_consume
      print " msgs/s\n"
    end
  end

  private def pub(done)
    data = IO::Memory.new(Bytes.new(@size))
    props = AMQ::Protocol::Properties.new(delivery_mode: @persistent ? 2u8 : nil)
    AMQP::Client.start(@uri) do |a|
      ch = a.channel
      Fiber.yield
      start = Time.monotonic
      pubs_this_second = 0
      until @stopped
        data.rewind
        if @confirm > 0
          ch.confirm_select
          msgid = ch.basic_publish data, @exchange, @routing_key, props: props
          ch.wait_for_confirm(msgid) if (msgid % @confirm) == 0
        else
          ch.basic_publish data, @exchange, @routing_key, props: props
        end
        @pubs += 1
        break if @pubs == @pmessages
        unless @rate.zero?
          pubs_this_second += 1
          if pubs_this_second >= @rate
            until_next_second = (start + 1.seconds) - Time.monotonic
            if until_next_second > Time::Span.zero
              sleep until_next_second
            end
            start = Time.monotonic
            pubs_this_second = 0
          end
        end
      end
    end
  ensure
    done.send nil
  end

  private def consume(done)
    AMQP::Client.start(@uri) do |a|
      ch = a.channel
      q = begin
        ch.queue @queue, args: @queue_args
      rescue
        ch = a.channel
        ch.queue(@queue, passive: true)
      end
      ch.prefetch @prefetch unless @prefetch.zero?
      q.bind(@exchange, @routing_key) unless @exchange.empty?
      Fiber.yield
      canceled = false
      consumes_this_second = 0
      start = Time.monotonic
      q.subscribe(tag: "c", no_ack: @ack.zero?, block: true, args: @consumer_args) do |m|
        @consumes += 1
        m.ack(multiple: true) if @ack > 0 && @consumes % @ack == 0
        if @stopped || @consumes == @cmessages
          ch.basic_cancel("c") unless canceled
          canceled = true
        end
        if @consume_rate.zero?
          Fiber.yield if @consumes % 128*1024 == 0
        else
          consumes_this_second += 1
          if consumes_this_second >= @consume_rate
            until_next_second = (start + 1.seconds) - Time.monotonic
            if until_next_second > Time::Span.zero
              sleep until_next_second
            end
            start = Time.monotonic
            consumes_this_second = 0
          end
        end
      end
    end
  ensure
    done.send nil
  end

  private def poll_consume(done)
    AMQP::Client.start(@uri) do |a|
      ch = a.channel
      q = begin
        ch.queue @queue
      rescue
        ch = a.channel
        ch.queue(@queue, passive: true)
      end
      ch.prefetch @prefetch unless @prefetch.zero?
      q.bind(@exchange, @routing_key) unless @exchange.empty?
      Fiber.yield
      consumes_this_second = 0
      start = Time.monotonic
      loop do
        if msg = q.get(no_ack: @ack.zero?)
          @consumes += 1
          msg.ack(multiple: true) if @ack > 0 && @consumes % @ack == 0
          break if @stopped || @consumes == @cmessages
        end
        unless @consume_rate.zero?
          consumes_this_second += 1
          if consumes_this_second >= @consume_rate
            until_next_second = (start + 1.seconds) - Time.monotonic
            if until_next_second > Time::Span.zero
              sleep until_next_second
            end
            start = Time.monotonic
            consumes_this_second = 0
          end
        end
      end
    end
  ensure
    done.send nil
  end
end

class BindChurn < Perf
  def run
    super

    r = Random.new
    AMQP::Client.start(@uri) do |c|
      ch = c.channel
      temp_q = ch.queue
      durable_q = ch.queue("durable")

      Benchmark.ips do |x|
        x.report("bind non-durable queue") do
          temp_q.bind "amq.direct", r.hex(16)
        end
        x.report("bind durable queue") do
          durable_q.bind "amq.direct", r.hex(10)
        end
      end
      durable_q.delete
    end
  end
end

class QueueChurn < Perf
  def run
    super

    AMQP::Client.start(@uri) do |c|
      ch = c.channel
      durable_q = ch.queue("durable")

      Benchmark.ips do |x|
        x.report("create/delete transient queue") do
          q = ch.queue
          q.delete
        end
        x.report("create/delete durable queue") do
          q = ch.queue("durable")
          q.delete
        end
      end
      durable_q.delete
    end
  end
end

class ConnectionChurn < Perf
  def run
    super
    c = AMQP::Client.new(@uri)
    Benchmark.ips do |x|
      x.report("open-close connection and channel") do
        conn = c.connect
        conn.channel
        conn.close
      end
    end
  end
end

class ChannelChurn < Perf
  def run
    super
    c = AMQP::Client.new(@uri)
    conn = c.connect
    Benchmark.ips do |x|
      x.report("open-close channel") do
        ch = conn.channel
        ch.close
      end
    end
  end
end

class ConsumerChurn < Perf
  def run
    super
    c = AMQP::Client.new(@uri).connect
    ch = c.channel
    q = ch.queue_declare "", auto_delete: false
    Benchmark.ips do |x|
      x.report("open-close consumer") do
        tag = ch.basic_consume(q[:queue_name]) { }
        ch.basic_cancel(tag, no_wait: true)
      end
    end
  end
end

class ConnectionCount < Perf
  alias BasicConsumeFrame = AMQP::Client::Frame::Basic::Consume
  @connections = 100
  @channels = 1
  @consumers = 0
  @queue = "connection-count"
  @localhost = false

  def initialize
    super
    @parser.on("-x count", "--count=number", "Number of connections (default 100)") do |v|
      @connections = v.to_i
    end
    @parser.on("-c channels", "--channels=number", "Number of channels per connection (default 1)") do |v|
      @channels = v.to_i
    end
    @parser.on("-C consumers", "--consumers=number", "Number of consumers per channel (default 0)") do |v|
      @consumers = v.to_i
    end
    @parser.on("-u queue", "--queue=name", "Queue name (default #{@queue})") do |v|
      @queue = v
    end
    @parser.on("-l", "--localhost", "Connect to localhost 127.0.0.0/16 guest/guest, for large number of local connections") do
      @localhost = true
    end
    @client = AMQP::Client.new(@uri)
  end

  def run
    super
    count = 0
    loop do
      start = Time.monotonic
      @connections.times do |i|
        c = client.connect
        @channels.times do |j|
          ch = c.channel
          @consumers.times do |k|
            ch.queue_declare @queue if i == j == k == 0
            # Send raw frame, no wait, no fiber
            c.write BasicConsumeFrame.new(ch.id, 0_u16, @queue, "", false, true, false, true, AMQP::Client::Arguments.new)
          end
        end
        print '.'
        if (i + 1) % 100 == 0
          print i + 1
          stop = Time.monotonic
          puts " #{(stop - start).total_milliseconds.round}ms"
          start = stop
        end
      end
      puts
      print "#{count += @connections} connections "
      print "#{count * @channels} channels "
      print "#{count * @channels * @consumers} consumers. "
      puts "Using #{rss.humanize_bytes} memory."
      puts "Press any key to do add #{@connections} connections (or ctrl-c to abort)"
      gets
    end
  end

  private def client
    if @localhost
      AMQP::Client.new(host: "127.0.#{Random.rand(UInt8)}.#{Random.rand(UInt8)}")
    else
      @client
    end
  end

  private def rss
    (`ps -o rss= -p $PPID`.to_i64? || 0i64) * 1024
  end
end

{% unless flag?(:release) %}
  STDERR.puts "WARNING: #{PROGRAM_NAME} not built in release mode"
{% end %}

mode = ARGV.shift?
case mode
when "throughput"       then Throughput.new.run
when "bind-churn"       then BindChurn.new.run
when "queue-churn"      then QueueChurn.new.run
when "connection-churn" then ConnectionChurn.new.run
when "channel-churn"    then ChannelChurn.new.run
when "consumer-churn"   then ConsumerChurn.new.run
when "connection-count" then ConnectionCount.new.run
when /^.+$/             then Perf.new.run([mode.not_nil!])
else                         abort Perf.new.banner
end
