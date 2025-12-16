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

    it "should parse basic arguments" do
      io = IO::Memory.new
      throughput = LavinMQPerf::AMQP::Throughput.new(io)

      # Parse arguments only - run method returns after parsing if no actual work
      throughput.run(["--uri=amqp://localhost", "-x", "1", "-y", "1"])
    end
  end

  describe "MQTT::Throughput" do
    it "should accept IO parameter" do
      io = IO::Memory.new
      throughput = LavinMQPerf::MQTT::Throughput.new(io)
      throughput.should_not be_nil
    end

    it "should parse basic arguments" do
      io = IO::Memory.new
      throughput = LavinMQPerf::MQTT::Throughput.new(io)

      throughput.run(["--uri=mqtt://localhost", "-x", "1", "-y", "1"])
    end
  end
end
