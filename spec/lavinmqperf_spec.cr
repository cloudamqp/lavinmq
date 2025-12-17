require "./spec_helper"
require "../src/lavinmqperf/perf"
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
      throughput = LavinMQPerf::AMQP::Throughput.new(io)

      # abort writes to STDERR and calls exit, which raises SpecExit in tests
      expect_raises(SpecExit) do
        throughput.run(["--invalid-flag"])
      end
    end

    it "should run throughput and produce output" do
      with_amqp_server do |s|
        # Set up TCP listener for AMQP connections
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port

        spawn(name: "amqp tcp listen") { s.listen(tcp_server, LavinMQ::Server::Protocol::AMQP) }
        Fiber.yield

        io = IO::Memory.new
        throughput = LavinMQPerf::AMQP::Throughput.new(io)

        # Spawn throughput test since it blocks
        spawn do
          throughput.run([
            "--uri=amqp://guest:guest@localhost:#{port}",
            "-x", "1",
            "-y", "1",
            "-z", "3",
          ])
        end

        # Wait for test to complete (3s + buffer)
        sleep 4.seconds

        output = io.to_s
        output.should_not be_empty

        # Parse per-second rates
        publish_rates = [] of Int32
        consume_rates = [] of Int32
        output.each_line do |line|
          if match = line.match(/Publish rate: (\d+) msgs\/s Consume rate: (\d+) msgs\/s/)
            publish_rates << match[1].to_i
            consume_rates << match[2].to_i
          end
        end

        # Verify we got rate reports (should be ~3 reports for a 3 second test)
        publish_rates.size.should be > 0
        consume_rates.size.should be > 0

        # Parse summary section and verify values are reasonable
        # Note: Summary calculates total_messages/total_time, which differs from
        # averaging per-second rates due to timing variations
        if match = output.match(/Average publish rate: (\d+) msgs\/s/)
          reported_avg_publish = match[1].to_i
          reported_avg_publish.should be > 0

          # Verify it's in a reasonable range (within 50% of the average of per-second rates)
          # This accounts for timing variations and the fact that summary uses total_msgs/total_time
          expected_avg_publish = publish_rates.sum / publish_rates.size
          (reported_avg_publish.to_f / expected_avg_publish).should be_close(1.0, 0.5)
        else
          fail "Summary missing average publish rate"
        end

        if match = output.match(/Average consume rate: (\d+) msgs\/s/)
          reported_avg_consume = match[1].to_i
          reported_avg_consume.should be > 0

          expected_avg_consume = consume_rates.sum / consume_rates.size
          (reported_avg_consume.to_f / expected_avg_consume).should be_close(1.0, 0.5)
        else
          fail "Summary missing average consume rate"
        end

        tcp_server.close
      end
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
      throughput = LavinMQPerf::MQTT::Throughput.new(io)

      # abort writes to STDERR and calls exit, which raises SpecExit in tests
      expect_raises(SpecExit) do
        throughput.run(["--invalid-flag"])
      end
    end
  end
end
