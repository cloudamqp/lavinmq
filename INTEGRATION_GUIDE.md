# Memory Management Integration Guide

This guide helps developers integrate the new memory management features into LavinMQ code.

## Quick Start

### 1. Using the Buffer Pool

The buffer pool is available through the Server instance:

```crystal
# In client/connection code
def process_message(message)
  buffer = @server.buffer_pool.acquire(message.size)
  begin
    # Use buffer for processing
    buffer.write(message.body)
    # ... process buffer ...
  ensure
    @server.buffer_pool.release(buffer)
  end
end
```

### 2. Memory Pressure Awareness

The memory pressure monitor runs automatically. Check pressure level if needed:

```crystal
if monitor = @server.memory_pressure_monitor
  stats = monitor.stats
  case stats[:level]
  when MemoryPressureMonitor::Level::High, MemoryPressureMonitor::Level::Critical
    # Defer non-critical operations
    # Reduce buffer sizes
    # Flush caches
  end
end
```

### 3. Enhanced MFile Operations

Use new madvise features for better memory management:

```crystal
# When opening message segment files
mfile = MFile.new(path, capacity)

# For sequential reads
mfile.prefetch(offset: 0, length: segment_size)

# When segment is no longer actively used
mfile.free_lazy

# For append-only segments
mfile.advise(MFile::Advice::Sequential)
```

## Best Practices

### Buffer Pool Usage

✅ **DO:**
- Use buffer pool for temporary IO operations
- Release buffers in ensure blocks
- Use appropriate buffer sizes (round up to nearest bucket)
- Check pool stats periodically to ensure effectiveness

❌ **DON'T:**
- Hold buffers for extended periods
- Store buffers in long-lived structures
- Acquire without releasing
- Use for very small (<1KB) or very large (>1MB) allocations

### Memory Pressure

✅ **DO:**
- Respect pressure levels in hot paths
- Defer non-critical work during high pressure
- Monitor pressure metrics in production
- Tune watermarks based on workload

❌ **DON'T:**
- Ignore critical pressure warnings
- Disable the monitor without good reason
- Set watermarks too low (causes excessive GC)
- Set watermarks too high (risks OOM)

### MFile Operations

✅ **DO:**
- Use prefetch for known sequential access patterns
- Call free_lazy on inactive segments
- Set appropriate advice hints based on access pattern
- Test on target platform (Linux vs BSD/Darwin)

❌ **DON'T:**
- Prefetch speculatively without measurement
- Call madvise in tight loops
- Assume all platforms support all hints
- Use DontNeed on actively accessed regions

## Monitoring

### Prometheus Metrics

Monitor these key metrics:

```bash
# Buffer pool effectiveness
lavinmq_buffer_pool_reuse_rate{bucket_size="16384"}

# Memory pressure
lavinmq_memory_pressure_level
lavinmq_memory_usage_ratio

# GC impact
lavinmq_memory_seconds_since_last_gc
lavinmq_gc_heap_size_bytes
```

### Alerts

Consider alerting on:
- Reuse rate < 50% (pool not effective)
- Pressure level >= 2 for > 5 minutes (sustained high pressure)
- Seconds since GC > 300 and pressure >= 1 (GC not triggering)

## Performance Tuning

### High Throughput Workloads

```crystal
# Increase pool sizes
@buffer_pool = BufferPool.new(max_per_bucket: 2000)

# Lower GC watermarks
@memory_pressure_monitor = MemoryPressureMonitor.new(
  @mem_limit,
  high_watermark: 0.65,
  critical_watermark: 0.85
)
```

### Memory Constrained Environments

```crystal
# Reduce pool sizes
@buffer_pool = BufferPool.new(max_per_bucket: 200)

# More aggressive GC
@memory_pressure_monitor = MemoryPressureMonitor.new(
  @mem_limit,
  high_watermark: 0.60,
  critical_watermark: 0.75,
  gc_cooldown: 5.seconds
)
```

### Large Messages

```crystal
# Add custom bucket sizes
class CustomBufferPool < BufferPool
  SIZE_BUCKETS = [1024, 4096, 16384, 65536, 262144, 1048576, 4194304]  # Added 4MB
end
```

## Testing

### Unit Tests

Test buffer pool integration:

```crystal
it "uses buffer pool for message processing" do
  server = create_test_server
  initial_stats = server.buffer_pool.stats[16384]
  
  process_messages(count: 100)
  
  final_stats = server.buffer_pool.stats[16384]
  reuse_rate = final_stats[:reuse_rate]
  reuse_rate.should be > 50.0  # Should reuse at least 50%
end
```

### Integration Tests

Test under memory pressure:

```crystal
it "handles memory pressure gracefully" do
  server = create_test_server
  monitor = server.memory_pressure_monitor.not_nil!
  
  # Simulate memory pressure by allocating
  fill_memory_to(75)  # 75% usage
  
  stats = monitor.stats
  stats[:level].should eq(MemoryPressureMonitor::Level::High)
  
  # Verify GC was triggered
  sleep 2.seconds
  stats[:seconds_since_last_gc].should be < 2.0
end
```

## Migration Path

For existing code that creates IO::Memory buffers:

### Before
```crystal
def handle_request(size)
  buffer = IO::Memory.new(size)
  # ... use buffer ...
end
```

### After
```crystal
def handle_request(size)
  buffer = @server.buffer_pool.acquire(size)
  begin
    # ... use buffer ...
  ensure
    @server.buffer_pool.release(buffer)
  end
end
```

## Troubleshooting

### Low Reuse Rate
- Check if buffers are being released
- Verify buffer sizes match common workload
- Ensure pool max_size is adequate

### High Memory Usage
- Check if memory pressure monitor is running
- Verify watermarks are appropriate
- Look for buffer leaks (acquired but not released)

### GC Thrashing
- Increase gc_cooldown period
- Raise high_watermark threshold
- Check if workload has sustained high allocation rate

## Future Enhancements

Planned improvements:
- Per-connection memory pools
- Adaptive bucket sizing
- NUMA-aware allocations
- Zero-copy message passing

See MEMORY_MANAGEMENT.md for more details.
