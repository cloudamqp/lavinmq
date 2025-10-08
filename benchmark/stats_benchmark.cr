require "benchmark"
require "../src/lavinmq/ring_buffer"

LOG_SIZE = 1024

# Deque-based implementation (original)
class DequeStats
  @rate_log = Deque(Float64).new(LOG_SIZE)
  @count_log = Deque(UInt32).new(LOG_SIZE)

  def update_rates(rate : Float64, count : UInt32) : Nil
    # Simulate the original pattern with size check and shift
    until @rate_log.size < LOG_SIZE
      @rate_log.shift
    end
    @rate_log.push rate

    until @count_log.size < LOG_SIZE
      @count_log.shift
    end
    @count_log.push count
  end

  def to_a
    {@rate_log.to_a, @count_log.to_a}
  end
end

# RingBuffer-based implementation (optimized with mask)
class RingBufferStats
  @rate_log = LavinMQ::RingBuffer(Float64).new(LOG_SIZE)
  @count_log = LavinMQ::RingBuffer(UInt32).new(LOG_SIZE)

  def update_rates(rate : Float64, count : UInt32) : Nil
    @rate_log.push rate
    @count_log.push count
  end

  def to_a
    {@rate_log.to_a, @count_log.to_a}
  end
end

puts "Stats Update Benchmark - Deque vs RingBuffer"
puts "Log size: #{LOG_SIZE}"
puts

# Benchmark with logs at capacity (typical steady-state)
# Pre-fill to capacity, then overwrite twice to ensure steady state
deque_stats_steady = DequeStats.new
ring_stats_steady = RingBufferStats.new
(LOG_SIZE * 3).times { |i| deque_stats_steady.update_rates(1.5, i.to_u32) }
(LOG_SIZE * 3).times { |i| ring_stats_steady.update_rates(1.5, i.to_u32) }

Benchmark.ips(2.seconds, 4.seconds) do |x|
  x.report("RingBuffer (steady-state)") do
    ring_stats_steady.update_rates(2.3, 42_u32)
  end

  x.report("Deque (steady-state)") do
    deque_stats_steady.update_rates(2.3, 42_u32)
  end
end

puts

# Benchmark fill from empty
Benchmark.ips(2.seconds, 4.seconds) do |x|
  x.report("RingBuffer (fill from empty)") do
    stats = RingBufferStats.new
    LOG_SIZE.times { |i| stats.update_rates(1.5, i.to_u32) }
  end

  x.report("Deque (fill from empty)") do
    stats = DequeStats.new
    LOG_SIZE.times { |i| stats.update_rates(1.5, i.to_u32) }
  end
end

puts

# Benchmark with to_a access (serialization)
Benchmark.ips(2.seconds, 4.seconds) do |x|
  deque_stats = DequeStats.new
  ring_stats = RingBufferStats.new

  LOG_SIZE.times { |i| deque_stats.update_rates(1.5, i.to_u32) }
  LOG_SIZE.times { |i| ring_stats.update_rates(1.5, i.to_u32) }

  x.report("RingBuffer (to_a)") do
    ring_stats.to_a
  end

  x.report("Deque (to_a)") do
    deque_stats.to_a
  end
end

puts
puts "Memory Usage Benchmark"
puts

# Empty structures
puts "Empty:"
puts "  RingBuffer: #{Benchmark.memory { RingBufferStats.new }}"
puts "  Deque: #{Benchmark.memory { DequeStats.new }}"

puts
puts "Full (#{LOG_SIZE} entries):"
puts "  RingBuffer: #{Benchmark.memory { stats = RingBufferStats.new; LOG_SIZE.times { |i| stats.update_rates(1.5, i.to_u32) } }}"
puts "  Deque: #{Benchmark.memory { stats = DequeStats.new; LOG_SIZE.times { |i| stats.update_rates(1.5, i.to_u32) } }}"
