require "benchmark"
require "../src/lavinmq/ring_buffer"

LOG_SIZE = 1024

class DequeStats
  @rate_log = Deque(Float64).new(LOG_SIZE)
  @count_log = Deque(UInt32).new(LOG_SIZE)

  def update_rates(rate : Float64, count : UInt32) : Nil
    until @rate_log.size < LOG_SIZE
      @rate_log.shift
    end
    @rate_log.push rate

    until @count_log.size < LOG_SIZE
      @count_log.shift
    end
    @count_log.push count
  end
end

class RingBufferStats
  @rate_log = LavinMQ::RingBuffer(Float64).new(LOG_SIZE)
  @count_log = LavinMQ::RingBuffer(UInt32).new(LOG_SIZE)

  def update_rates(rate : Float64, count : UInt32) : Nil
    @rate_log.push rate
    @count_log.push count
  end
end

puts "Memory Usage Benchmark"
puts "Log size: #{LOG_SIZE}"
puts

puts "Empty:"
puts "  RingBuffer: #{Benchmark.memory { RingBufferStats.new }}"
puts "  Deque: #{Benchmark.memory { DequeStats.new }}"

puts
puts "Full (#{LOG_SIZE} entries):"
puts "  RingBuffer: #{Benchmark.memory { stats = RingBufferStats.new; LOG_SIZE.times { |i| stats.update_rates(1.5, i.to_u32) } }}"
puts "  Deque: #{Benchmark.memory { stats = DequeStats.new; LOG_SIZE.times { |i| stats.update_rates(1.5, i.to_u32) } }}"
