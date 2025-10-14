require "benchmark"
require "http/server"
require "../src/lavinmq/ring_buffer"
require "../src/stdlib/deque"
require "json"

# Simulated stats structure with RingBuffer
class StatsWithRingBuffer
  getter message_rate_log : LavinMQ::RingBuffer(Float64)
  getter connection_log : LavinMQ::RingBuffer(UInt32)

  def initialize(size : Int32)
    @message_rate_log = LavinMQ::RingBuffer(Float64).new(size)
    @connection_log = LavinMQ::RingBuffer(UInt32).new(size)

    # Fill with sample data
    size.times do |i|
      @message_rate_log.push(100.0 + i % 50)
      @connection_log.push((10 + i % 20).to_u32)
    end
  end

  def to_json(json : JSON::Builder)
    json.object do
      json.field "message_rates", @message_rate_log.to_a
      json.field "connections", @connection_log.to_a
    end
  end

  def to_json
    String.build do |io|
      json = JSON::Builder.new(io)
      json.document do
        to_json(json)
      end
    end
  end
end

# Simulated stats structure with Deque
class StatsWithDeque
  getter message_rate_log : Deque(Float64)
  getter connection_log : Deque(UInt32)

  def initialize(size : Int32)
    @message_rate_log = Deque(Float64).new(size)
    @connection_log = Deque(UInt32).new(size)

    # Fill with sample data
    size.times do |i|
      @message_rate_log.push(100.0 + i % 50)
      @connection_log.push((10 + i % 20).to_u32)
    end
  end

  def to_json(json : JSON::Builder)
    json.object do
      json.field "message_rates", @message_rate_log.to_a
      json.field "connections", @connection_log.to_a
    end
  end

  def to_json
    String.build do |io|
      json = JSON::Builder.new(io)
      json.document do
        to_json(json)
      end
    end
  end
end

log_size = 1024
puts "HTTP Stats Benchmark - RingBuffer vs Deque"
puts "Log size: #{log_size}"
puts "Testing JSON serialization (simulating UI refresh)"
puts

# Create instances
stats_rb = StatsWithRingBuffer.new(log_size)
stats_deque = StatsWithDeque.new(log_size)

# Benchmark to_a operation directly
puts "Direct to_a benchmark:"
Benchmark.ips do |x|
  x.report("RingBuffer to_a") do
    stats_rb.message_rate_log.to_a
    stats_rb.connection_log.to_a
  end

  x.report("Deque to_a") do
    stats_deque.message_rate_log.to_a
    stats_deque.connection_log.to_a
  end
end

puts "\nJSON serialization benchmark (full UI response):"
Benchmark.ips do |x|
  x.report("RingBuffer JSON") do
    stats_rb.to_json
  end

  x.report("Deque JSON") do
    stats_deque.to_json
  end
end

# Simulate concurrent requests like UI polling
puts "\nConcurrent request simulation (10 parallel requests):"
channel_rb = Channel(Nil).new
channel_deque = Channel(Nil).new

rb_time = Time.measure do
  10.times do
    spawn do
      100.times do
        stats_rb.to_json
      end
      channel_rb.send(nil)
    end
  end
  10.times { channel_rb.receive }
end

deque_time = Time.measure do
  10.times do
    spawn do
      100.times do
        stats_deque.to_json
      end
      channel_deque.send(nil)
    end
  end
  10.times { channel_deque.receive }
end

puts "RingBuffer: #{rb_time.total_milliseconds.round(2)}ms"
puts "Deque: #{deque_time.total_milliseconds.round(2)}ms"
puts "Speedup: #{(deque_time / rb_time).round(2)}×"

# Test with different sizes to see scaling
puts "\nScaling test (JSON serialization):"
[256, 512, 1024, 2048, 4096].each do |size|
  rb = StatsWithRingBuffer.new(size)
  dq = StatsWithDeque.new(size)

  rb_time = Time.measure do
    1000.times { rb.to_json }
  end

  dq_time = Time.measure do
    1000.times { dq.to_json }
  end

  speedup = (dq_time / rb_time).round(2)
  puts "Size #{size}: RB=#{rb_time.total_milliseconds.round(2)}ms, DQ=#{dq_time.total_milliseconds.round(2)}ms, Speedup=#{speedup}×"
end
