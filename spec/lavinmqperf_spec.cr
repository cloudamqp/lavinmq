require "./spec_helper"
require "../src/lavinmqperf/perf"
require "../src/lavinmqperf/ticker"
require "../src/lavinmqperf/amqp/*"
require "../src/lavinmqperf/mqtt/*"

# Monkey-patch to redirect output to captured IO
module LavinMQPerf
  class Perf
    def puts(*args, **options)
      @io.puts(*args, **options)
    end

    def print(*args)
      @io.print(*args)
    end
  end
end

describe "LavinMQPerf" do
  describe "Perf base class" do
    it "should have AMQP banner" do
      perf = LavinMQPerf::Perf.new
      perf.amqp_banner.should contain("throughput")
      perf.amqp_banner.should contain("bind-churn")
      perf.amqp_banner.should contain("queue-churn")
    end

    it "should have MQTT banner" do
      perf = LavinMQPerf::Perf.new
      perf.mqtt_banner.should contain("throughput")
    end

    it "should capture output with IO parameter" do
      io = IO::Memory.new
      perf = LavinMQPerf::Perf.new(io)
      perf.amqp_banner.should_not be_empty
    end
  end

  describe "AMQP::Throughput" do
    it "should accept IO parameter" do
      io = IO::Memory.new
      throughput = LavinMQPerf::AMQP::Throughput.new(io)
      throughput.should_not be_nil
    end

    it "should fail with invalid argument" do
      io = IO::Memory.new
      throughput = LavinMQPerf::AMQP::Throughput.new(io, io)

      # abort writes to err_io and calls exit, which raises SpecExit in tests
      expect_raises(SpecExit) do
        throughput.run(["--invalid-flag"])
      end
    end

    it "should verify summary averages are correct", tags: "slow" do
      with_amqp_server do |s|
        io = IO::Memory.new
        throughput = LavinMQPerf::AMQP::Throughput.new(io)

        throughput.run([
          "--uri=amqp://guest:guest@localhost:#{amqp_port(s)}",
          "-x", "1",
          "-y", "1",
          "-z", "3",
        ])

        # Parse output line by line
        io.rewind
        publish_rates = [] of Int32
        consume_rates = [] of Int32
        reported_avg_publish = 0
        reported_avg_consume = 0

        while line = io.gets
          if match = line.match(/Publish rate: (\d+) msgs\/s Consume rate: (\d+) msgs\/s/)
            publish_rates << match[1].to_i
            consume_rates << match[2].to_i
          elsif match = line.match(/Average publish rate: (\d+) msgs\/s/)
            reported_avg_publish = match[1].to_i
          elsif match = line.match(/Average consume rate: (\d+) msgs\/s/)
            reported_avg_consume = match[1].to_i
          end
        end

        # Verify we got rate reports (should be ~3 reports for a 3 second test)
        publish_rates.size.should be > 0
        consume_rates.size.should be > 0

        # Verify summary values are in a reasonable range (within 50% of reported rates)
        # This accounts for timing variations and difference in calculations
        reported_avg_publish.should be > 0
        measured_avg_publish = publish_rates.sum / publish_rates.size
        (reported_avg_publish.to_f / measured_avg_publish).should be_close(1.0, 0.5)
        reported_avg_consume.should be > 0
        measured_avg_consume = consume_rates.sum / consume_rates.size
        (reported_avg_consume.to_f / measured_avg_consume).should be_close(1.0, 0.5)
      end
    end

    it "shares connections among clients with --connections", tags: "slow" do
      with_amqp_server do |s|
        io = IO::Memory.new
        throughput = LavinMQPerf::AMQP::Throughput.new(io)
        done = Channel(Nil).new
        spawn do
          throughput.run([
            "--uri=amqp://guest:guest@localhost:#{amqp_port(s)}",
            "-x", "2", "-y", "3", "-r", "10", "-a", "1", "-P", "10",
            "--queue-pattern", "shared-%", "--queue-pattern-to", "3",
            "--connections", "2", "-z", "3",
          ])
          done.send nil
        end
        wait_for { s.vhosts["/"].queues_size == 3 }
        wait_for { s.connections.sum(&.as(LavinMQ::AMQP::Client).channels.size) == 5 }
        s.connections.size.should eq 2
        done.receive
      end
    end

    it "consumes a backlog on every queue with shared connections", tags: "slow" do
      with_amqp_server do |s|
        with_channel(s) do |ch|
          10.times do |i|
            q = ch.queue("backlog-#{i + 1}")
            100.times { q.publish "x" }
          end
        end
        io = IO::Memory.new
        throughput = LavinMQPerf::AMQP::Throughput.new(io)
        throughput.run([
          "--uri=amqp://guest:guest@localhost:#{amqp_port(s)}",
          "-x", "0", "-y", "10", "-R", "10", "-a", "1", "-P", "10",
          "--queue-pattern", "backlog-%", "--queue-pattern-to", "10",
          "--connections", "1", "-z", "2",
        ])
        # Every consumer gets its share; none is left holding a first
        # prefetch window it never saw.
        10.times do |i|
          q = s.vhosts["/"].queue("backlog-#{i + 1}")
          (q.message_count + q.unacked_count).should be < 95
        end
      end
    end

    it "publishes and consumes at fractional rates", tags: "slow" do
      with_amqp_server do |s|
        io = IO::Memory.new
        throughput = LavinMQPerf::AMQP::Throughput.new(io)
        throughput.run([
          "--uri=amqp://guest:guest@localhost:#{amqp_port(s)}",
          "-x", "1", "-y", "1", "-r", "2.5", "-R", "2.5", "-a", "1", "-P", "10",
          "-u", "fractional", "-z", "4",
        ])
        # Both ends at 2.5 per second: what's left is in flight.
        q = s.vhosts["/"].queue("fractional")
        io.to_s.should match(/Average publish rate: [1-3] msgs\/s/)
        (q.message_count + q.unacked_count).should be <= 2
      end
    end
  end

  describe "AMQP::ChannelChurn" do
    it "opens and closes channels at a fixed rate with --rate", tags: "slow" do
      with_amqp_server do |s|
        io = IO::Memory.new
        churn = LavinMQPerf::AMQP::ChannelChurn.new(io)
        spawn do
          churn.run(["--uri=amqp://guest:guest@localhost:#{amqp_port(s)}", "--rate", "4"])
        rescue ::AMQP::Client::Error | IO::Error
          # the server stops at the end of the spec
        end
        wait_for { io.to_s.includes?("Channels opened and closed:") }
        sleep 1.seconds
        count = io.to_s.scan(/Channels opened and closed: (\d+)/).last[1].to_i
        count.should be >= 4
        count.should be <= 12
        s.connections.size.should eq 1
      end
    end
  end

  describe "Ticker" do
    it "paces fractional rates" do
      ticker = LavinMQPerf::Ticker.new(2.5)
      start = Time.instant
      3.times { ticker.tick }
      (Time.instant - start).should be >= (3 / 2.5).seconds * 0.7
    end

    # Bursty pacing puts all ops in the first bucket; smooth pacing spreads
    # them evenly. Lower bounds only, so a slow CI just stretches the run.
    it "paces evenly across the configured rate window" do
      rate = 500
      total_ops = 250
      ticker = LavinMQPerf::Ticker.new(rate)
      timestamps = Array(Time::Span).new(total_ops)
      start = Time.instant
      total_ops.times do
        ticker.tick
        timestamps << Time.instant - start
      end
      elapsed = Time.instant - start

      target = (total_ops.to_f64 / rate).seconds
      elapsed.should be >= target * 0.7

      bucket_count = 10
      bucket_width = elapsed / bucket_count
      buckets = Array(Int32).new(bucket_count, 0)
      timestamps.each do |ts|
        idx = (ts.total_nanoseconds / bucket_width.total_nanoseconds).to_i
        idx = bucket_count - 1 if idx >= bucket_count
        buckets[idx] += 1
      end
      buckets.max.should be < total_ops // 2
    end

    # The consume path hands the ticker to a helper method per message, so it
    # must keep state across calls — a value type would reset every call and
    # skip pacing. Ticking through an indirection locks that in.
    it "keeps pacing when ticked through another method" do
      ticker = LavinMQPerf::Ticker.new(500)
      tick = ->(t : LavinMQPerf::Ticker) { t.tick }
      start = Time.instant
      250.times { tick.call(ticker) }
      (Time.instant - start).should be >= (250.0 / 500).seconds * 0.7
    end
  end

  describe "MQTT::Throughput" do
    it "should accept IO parameter" do
      io = IO::Memory.new
      throughput = LavinMQPerf::MQTT::Throughput.new(io)
      throughput.should_not be_nil
    end

    it "should fail with invalid argument" do
      io = IO::Memory.new
      throughput = LavinMQPerf::MQTT::Throughput.new(io, io)

      # abort writes to err_io and calls exit, which raises SpecExit in tests
      expect_raises(SpecExit) do
        throughput.run(["--invalid-flag"])
      end
    end
  end
end
