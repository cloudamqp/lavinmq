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
  end

  describe "Ticker" do
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
