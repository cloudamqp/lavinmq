# Memory Management Improvements

## Overview

LavinMQ has been enhanced with advanced memory management techniques inspired by Bun's high-performance memory management strategies. These improvements focus on reducing allocation overhead, improving memory pressure handling, and optimizing memory-mapped file operations.

## Key Features

### 1. Object Pooling (`src/lavinmq/memory_pool.cr`)

#### Generic Memory Pool
A thread-safe object pool that reduces allocation overhead by reusing objects:

- **Thread-safe design**: Uses mutex for concurrent access
- **Configurable size**: Maximum pool size to prevent unbounded growth
- **Statistics tracking**: Monitors creation, reuse rates, and pool efficiency
- **Atomic counters**: Lock-free statistics for minimal overhead

#### Buffer Pool
A specialized pool for IO::Memory buffers with size-based bucketing:

- **Size buckets**: 1KB, 4KB, 16KB, 64KB, 256KB, 1MB
- **Automatic sizing**: Selects appropriate bucket for requested size
- **Buffer reuse**: Reduces garbage collection pressure for message processing
- **Per-bucket statistics**: Track effectiveness of each bucket

**Benefits:**
- Reduces allocation churn during high message throughput
- Decreases GC pressure
- Improves cache locality

### 2. Memory Pressure Monitoring (`src/lavinmq/memory_pressure.cr`)

Advanced memory pressure detection and response system:

#### Pressure Levels
- **Normal**: < 60% of high watermark
- **Moderate**: 60-75% of memory limit
- **High**: 75-90% of memory limit
- **Critical**: > 90% of memory limit

#### Features
- **Adaptive GC triggering**: Automatically triggers garbage collection based on memory pressure
- **Cooldown periods**: Prevents excessive GC cycles
- **Configurable watermarks**: Tune thresholds for your workload
- **Detailed logging**: Track memory usage patterns

**Behavior:**
- **Normal**: No action
- **Moderate**: Logging only
- **High**: Trigger GC if cooldown period has passed
- **Critical**: Force aggressive GC immediately

### 3. Enhanced Memory-Mapped File Operations (`src/lavinmq/mfile.cr`)

Improved memory advice hints for better OS-level memory management:

#### New Features

**Lazy Free** - Tell OS this memory can be lazily freed
**Prefetching** - Prefetch pages that will be needed soon

**Additional Memory Advice Hints (Linux):**
- `MADV_FREE`: Lazy free for faster reclamation
- `MADV_REMOVE`: Free backing store
- `MADV_HUGEPAGE`: Use huge pages for better performance
- `MADV_NOHUGEPAGE`: Disable huge pages
- `MADV_MERGEABLE`: Enable Kernel Samepage Merging (KSM)

**Benefits:**
- Better cache utilization
- Reduced memory pressure
- Improved sequential read performance
- More efficient memory reclamation

### 4. Prometheus Metrics Integration

New metrics exposed at `/metrics` endpoint:

#### Buffer Pool Metrics
- `lavinmq_buffer_pool_size{bucket_size="4096"}`: Current pool size per bucket
- `lavinmq_buffer_pool_total_created{bucket_size="4096"}`: Total buffers created
- `lavinmq_buffer_pool_total_reused{bucket_size="4096"}`: Total buffers reused
- `lavinmq_buffer_pool_reuse_rate{bucket_size="4096"}`: Reuse rate percentage

#### Memory Pressure Metrics
- `lavinmq_memory_pressure_level`: Current pressure level (0-3)
- `lavinmq_memory_usage_ratio`: Memory usage ratio (0.0-1.0)
- `lavinmq_memory_limit_bytes`: Configured memory limit
- `lavinmq_memory_seconds_since_last_gc`: Time since last GC

## Performance Impact

### Expected Improvements

1. **Reduced Allocation Overhead**
   - 50-80% reduction in buffer allocations for high-throughput scenarios
   - Lower GC pause times due to reduced allocation pressure

2. **Better Memory Utilization**
   - Proactive memory pressure response prevents OOM conditions
   - More predictable memory usage patterns

3. **Improved Throughput**
   - Buffer reuse reduces allocation latency
   - Better CPU cache utilization with pooled objects

4. **Enhanced Observability**
   - Real-time memory pressure monitoring
   - Buffer pool effectiveness tracking

## Comparison to Bun

| Feature | Bun | LavinMQ Implementation |
|---------|-----|----------------------|
| Thread-local pools | ✓ | ✓ (via Mutex-protected pools in multi-threaded mode) |
| Buffer pooling | ✓ | ✓ (Size-bucketed buffer pools) |
| Memory pressure awareness | ✓ | ✓ (4-level pressure monitoring) |
| Specialized allocators | ✓ | ○ (Uses Crystal's GC, but adds pooling layer) |
| Cache-friendly layouts | ✓ | ✓ (Memory-mapped files with advice hints) |
| OS-level optimizations | ✓ | ✓ (Enhanced madvise usage) |

## Future Improvements

1. **Arena Allocators**: Per-connection or per-queue memory arenas
2. **Zero-Copy Message Passing**: Eliminate buffer copies between stages
3. **Adaptive Pool Sizing**: Dynamically adjust pool sizes based on workload
4. **NUMA Awareness**: Optimize for multi-socket systems
5. **Transparent Huge Pages**: Automatic huge page management

## References

- [Bun's Memory Management](https://bun.com/blog/behind-the-scenes-of-bun-install)
- [Linux Memory Advice](https://man7.org/linux/man-pages/man2/madvise.2.html)
- [Crystal GC Documentation](https://crystal-lang.org/reference/guides/performance.html)
