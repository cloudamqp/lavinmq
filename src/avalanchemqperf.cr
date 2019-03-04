require "./avalanchemq/version"
require "./avalanchemq/io_tweak"
require "option_parser"
require "amqp-client"
require "benchmark"

class Perf
  @uri = "amqp://guest:guest@localhost"
  getter banner

  def initialize
    @parser = OptionParser.new
    @banner = "Usage: #{PROGRAM_NAME} [throughput | bind-churn | queue-churn | connection-churn] [arguments]"
    @parser.banner = @banner
    @parser.on("-h", "--help", "Show this help") { puts @parser; exit 1 }
    @parser.on("-v", "--version", "Show version") { puts AvalancheMQ::VERSION; exit 0 }
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
  @size = 1024
  @exchange = ""
  @queue = "perf-test"
  @routing_key = "perf-test"
  @no_ack = true
  @rate = 0
  @consume_rate = 0
  @confirm = false

  def initialize
    super
    @parser.on("-x publishers", "--publishers=number", "Number of publishers (default 1)") do |v|
      @publishers = v.to_i
    end
    @parser.on("-y consumers", "--consumers=number", "Number of consumers (default 1)") do |v|
      @consumers = v.to_i
    end
    @parser.on("-s msgsize", "--size=bytes", "Size of each message (default 1KB)") do |v|
      @size = v.to_i
    end
    @parser.on("-a", "--ack", "Ack consumed messages (default false)") do
      @no_ack = false
    end
    @parser.on("-c", "--confirm", "Confirm publishes (default false)") do
      @confirm = true
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
  end

  @pubs = 0
  @consumes = 0

  def run
    super

    @publishers.times do
      spawn pub
    end

    @consumers.times do
      spawn consume
    end

    loop do
      sleep 1
      print "Publish rate: "
      print @pubs
      print " msgs/s Consume rate: "
      print @consumes
      print " msgs/s\n"
      @pubs = 0
      @consumes = 0
    end
  end

  private def pub
    a = AMQP::Client.new(@uri).connect
    ch = a.channel
    data = "0" * @size
    loop do
      if @confirm
        ch.basic_publish_confirm data, @exchange, @routing_key
      else
        ch.basic_publish data, @exchange, @routing_key
      end
      @pubs += 1
      if @rate.zero?
        Fiber.yield if @pubs % 1000 == 0
      else
        sleep 1.0 / @rate
      end
    end
  ensure
    a.try &.close
  end

  private def consume
    a = AMQP::Client.new(@uri).connect
    ch = a.channel
    ch.queue(@queue).subscribe(no_ack: @no_ack) do |m|
      m.ack unless @no_ack
      @consumes += 1
      if @consume_rate.zero?
        Fiber.yield if @consumes % 1000 == 0
      else
        sleep 1.0 / @consume_rate
      end
    end
    sleep
  ensure
    a.try &.close
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
    Benchmark.ips do |x|
      x.report("open-close connection and channel") do
        AMQP::Client.start(@uri) do |c|
          c.channel
        end
      end
    end
  end
end

arg = ARGV.shift?
case arg
when "throughput"       then Throughput.new.run
when "bind-churn"       then BindChurn.new.run
when "queue-churn"      then QueueChurn.new.run
when "connection-churn" then ConnectionChurn.new.run
when /^.+$/             then Perf.new.run([arg.not_nil!])
else                         abort Perf.new.banner
end
