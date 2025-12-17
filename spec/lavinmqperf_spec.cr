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

    it "should verify summary averages are correct" do
      with_amqp_server do |s|
        # Set up TCP listener for AMQP connections
        tcp_server = TCPServer.new("localhost", 0)
        port = tcp_server.local_address.port
        spawn(name: "amqp tcp listen") { s.listen(tcp_server, LavinMQ::Server::Protocol::AMQP) }

        io = IO::Memory.new
        throughput = LavinMQPerf::AMQP::Throughput.new(io)

        throughput.run([
          "--uri=amqp://guest:guest@localhost:#{port}",
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
