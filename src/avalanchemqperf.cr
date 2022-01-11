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
    @parser.on("--uri=URI", "URI to connect to (default amqp://guest:guest@localhost)") do |v|
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
    props = AMQ::Protocol::Properties.new
    props.delivery_mode = 2_u8 if @persistent
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
        ch.queue @queue
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
      q.subscribe(tag: "c", no_ack: @ack.zero?, block: true) do |m|
        @consumes += 1
        m.ack(multiple: true) if @ack > 0 && @consumes % @ack == 0
        if @stopped || @consumes == @cmessages
          ch.basic_cancel("c") unless canceled
          canceled = true
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
when /^.+$/             then Perf.new.run([mode.not_nil!])
else                         abort Perf.new.banner
end
