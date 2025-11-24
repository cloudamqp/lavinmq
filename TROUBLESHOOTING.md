# LavinMQ Troubleshooting Guide

This guide helps you troubleshoot LavinMQ issues, gather debugging information, and create detailed bug reports for the LavinMQ open source repository.

## Quick Diagnostics

When experiencing issues with LavinMQ, start by gathering basic information:

```sh
# Check if LavinMQ is running
systemctl status lavinmq.service

# Check recent logs
journalctl -fu lavinmq --lines 100

# Check LavinMQ version
lavinmq --version

# Check system resources
df -h /var/lib/lavinmq  # Disk space
free -h                  # Memory
```

## Gathering Debug Information

### Dump Server State (USR1 Signal)

USR1 signal dumps detailed information about LavinMQ's current state. This is **very useful** when reporting bugs.

```sh
# Send USR1 signal to LavinMQ
kill -USR1 $(pidof lavinmq)

# Or using killall
killall -USR1 lavinmq
```

**What USR1 dumps:**
- **Garbage Collector statistics** - heap size, free bytes, unmapped bytes, GC cycle number
- **Fiber list** - all running fibers and their states (useful for detecting deadlocks or stuck operations)
- **Complete server state**:
  - Users and their permissions
  - VHosts and their resources
  - Exchanges (size and capacity)
  - Queues (name, durability, arguments, message counts, segments)
  - Connections and channels
  - Consumers and unacked messages
  - ACL caches

**Where to find the output:**

The output goes to `stdout`, which if the LavinMQ is managed by SystemD the output is captured by `journald`:

```sh
kill -USR1 $(pidof lavinmq)
journalctl -eu lavinmq
```

**When to use USR1:**
- Before reporting a bug
- When experiencing performance issues
- When connections or queues seem stuck
- When investigating memory issues
- Before and after reproducing a problem

### Manual Garbage Collection (USR2 Signal)

Force LavinMQ to run garbage collection. Useful when investigating memory-related issues.

```sh
killall -USR2 lavinmq
```

This can help determine if high memory usage is due to actual data or uncollected garbage.

## Log Analysis

LavinMQ outputs all logs to `stdout`, which are captured by `journald`.

### Basic Log Commands

```sh
# Follow logs in real-time
journalctl -fu lavinmq

# View recent logs
journalctl -u lavinmq --since "1 hour ago"

# View logs from a specific time range
journalctl -u lavinmq --since "2025-01-20 10:00:00" --until "2025-01-20 11:00:00"

# Search for specific errors
journalctl -u lavinmq --grep "error|exception"

# Show only errors and warnings
journalctl -u lavinmq -p err

# Export logs to a file for sharing
journalctl -u lavinmq --since today > lavinmq_logs.txt
```

### Log Navigation

When viewing logs with `journalctl`:
- `j/k` - Scroll down/up
- `Shift+g` - Jump to latest logs
- `g` - Jump to oldest logs
- `b` - Move up one page
- `Space` - Move down one page
- `/pattern` - Search forward for pattern
- `?pattern` - Search backward for pattern
- `q` - Exit

### What to Look For

- **Exceptions and stack traces** - Crystal stack traces showing the error location
- **Connection errors** - Authentication failures, protocol errors
- **Disk errors** - I/O errors, disk full warnings
- **Memory pressure** - OOM warnings
- **Cluster issues** - Replication errors, leader election problems

## Network and Connection Debugging

### Using lsof

`lsof` (List Open Files) shows all open files and network connections for LavinMQ.

```sh
# List all open files by LavinMQ
sudo lsof -p $(pidof lavinmq)

# Show only network connections
sudo lsof -p $(pidof lavinmq) -i

# Count open connections by type
sudo lsof -p $(pidof lavinmq) -i | awk '{print $8}' | sort | uniq -c

# Show TCP connections
sudo lsof -p $(pidof lavinmq) -i TCP

# Show connections to specific port
sudo lsof -p $(pidof lavinmq) -i :5672

# Count total file descriptors
sudo lsof -p $(pidof lavinmq) | wc -l
```

**Common uses:**
- Verify LavinMQ is listening on expected ports (5672, 15672, 1883)
- Count active client connections
- Check for connection leaks
- Identify file descriptor exhaustion

### TCP Connection States

```sh
# Show connection states
sudo ss -antp | grep $(pidof lavinmq)

# Count connections by state
sudo ss -antp | grep $(pidof lavinmq) | awk '{print $1}' | sort | uniq -c
```

## Performance Debugging

### Using perf top

`perf top` shows real-time CPU usage by function, helping identify performance bottlenecks and infinite loops.

**Installation (not installed by default):**

```sh
sudo apt install linux-tools-common
sudo apt install linux-tools-aws  # or linux-tools-generic depending on kernel
```

**Usage:**

```sh
# Monitor LavinMQ CPU usage in real-time
sudo perf top -p $(pidof lavinmq)
```

**What to look for:**
- Functions consuming high CPU percentage
- Tight loops or repeated function calls
- Unexpected hot paths

**Important:** If LavinMQ becomes unresponsive, take a screenshot of `perf top` output before restarting. This is crucial for debugging hangs and performance issues.

### Recording Performance Data

For persistent performance issues, record data for later analysis:

```sh
# Record performance data (run for 30-60 seconds)
sudo perf record --call-graph lbr -p $(pidof lavinmq)

# Stop recording with Ctrl+C after sufficient time

# View the report
sudo perf report

# The recording is saved to perf.data
ls -lh perf.data
```

**Send `perf.data` to developers** when reporting performance issues - it contains detailed call graph information.

### perf Navigation

When viewing `perf report`:
- `Arrow keys` - Navigate up/down
- `Enter` - Expand call chain
- `+` - Expand all
- `-` - Collapse all
- `/` - Search
- `?` - Help
- `q` - Quit

## Core Dump Analysis

When LavinMQ crashes with a segmentation fault, a core dump captures the exact state of memory at crash time. LavinMQ allows the OS to generate core dumps (see `Signal::SEGV.reset` in src/lavinmq/launcher.cr:282), but **you need to enable core dump capture before reproducing the crash**.

### Enabling Core Dumps for Debugging

**Before trying to reproduce a segfault:**

```sh
# Check if core dumps are currently enabled
ulimit -c
# If it shows 0, core dumps are disabled
```

**To capture a core dump when debugging LavinMQ:**

If running LavinMQ manually for testing:

```sh
# In your shell, enable core dumps with a size limit (e.g., 2GB)
ulimit -c 2097152  # 2GB in kilobytes

# Now run LavinMQ
./bin/lavinmq -D /path/to/data

# When it crashes, the core dump will be captured
```

If LavinMQ is running as a systemd service, temporarily enable core dumps:

```sh
# Set core dump limit for this session
sudo sh -c 'ulimit -c 2097152 && systemctl restart lavinmq'
```

**After debugging:** Reset the limit to avoid filling disk:

```sh
ulimit -c 0
```

### Checking for Core Dumps

```sh
# List recent core dumps
coredumpctl list

# Show info about the latest crash
coredumpctl info

# Show detailed crash information
coredumpctl info lavinmq
```

### Analyzing with GDB

```sh
# Open the core dump in GDB
sudo coredumpctl gdb lavinmq

# In GDB, useful commands:
# bt           - Show backtrace (stack trace)
# bt full      - Show backtrace with local variables
# frame N      - Switch to frame N
# print var    - Print variable value
# info threads - Show all threads
# thread N     - Switch to thread N
# quit         - Exit GDB
```

### Exporting Core Dumps

```sh
# Save core dump to file for sharing
coredumpctl dump lavinmq > lavinmq_core.dump

# Or use -o flag
coredumpctl dump lavinmq -o lavinmq_core.dump
```

**Note:** Core dumps can be very large. Compress before sharing:

```sh
gzip lavinmq_core.dump
```

## System Resource Checks

### Disk Space

LavinMQ stores all messages on disk. A full disk will prevent LavinMQ from accepting new messages.

```sh
# Check disk usage of data directory
du -sh /var/lib/lavinmq/*

# Check disk space
df -h /var/lib/lavinmq

# Find largest queue directories
du -sh /var/lib/lavinmq/*/* | sort -h | tail -20

# Check for large message segment files
find /var/lib/lavinmq -name "msgs.*" -exec ls -l {} \; | sort -k5 -n | tail -20
```

### Memory

High memory usage may indicate a memory leak, too many unacked messages, or normal behavior under heavy load.

```sh
# Check LavinMQ memory usage
ps aux | grep lavinmq

# More detailed memory info
cat /proc/$(pidof lavinmq)/status | grep -i vmsize
cat /proc/$(pidof lavinmq)/status | grep -i vmrss

# Memory maps (can show memory leaks)
sudo pmap -x $(pidof lavinmq)
```

### File Descriptors

LavinMQ needs 1 file descriptor per connection + 2 per durable queue + overhead. Running out of FDs prevents accepting new connections or creating queues.

```sh
# Check FD limit
cat /proc/$(pidof lavinmq)/limits | grep "open files"

# Count currently open FDs
sudo ls -l /proc/$(pidof lavinmq)/fd | wc -l

# Show what FDs are open
sudo ls -l /proc/$(pidof lavinmq)/fd
```

### Memory Map Areas

LavinMQ uses memory-mapped files for message storage (1 mmap per connection + 1 per message segment). With many connections or queues, you can hit the OS limit (default ~65k), preventing new connections or segment creation.

```sh
# Check current limit
cat /proc/sys/vm/max_map_count

# Increase limit temporarily for debugging
sudo sysctl -w vm.max_map_count=1000000

# Make permanent if needed (add to /etc/sysctl.conf)
echo "vm.max_map_count=1000000" | sudo tee -a /etc/sysctl.conf
```

## Cluster Debugging

For clustered LavinMQ instances:

```sh
# Check cluster status via API
curl -u guest:guest http://localhost:15672/api/cluster

# Check etcd ISR (In-Sync Replicas)
etcdctl get lavinmq/isr
```

## Creating a Bug Report

When creating an issue on GitHub (https://github.com/cloudamqp/lavinmq/issues), include:

### Essential Information

1. **LavinMQ version**
   ```sh
   lavinmq --version
   ```

2. **Operating system**
   ```sh
   cat /etc/os-release
   uname -a
   ```

3. **Server state dump (USR1)**
   ```sh
   kill -USR1 $(pidof lavinmq)
   journalctl -u lavinmq --since "1 minute ago" > server_state.txt
   ```

4. **Relevant logs**
   ```sh
   journalctl -u lavinmq --since "1 hour ago" > lavinmq_logs.txt
   ```

5. **Stack trace** (if there was a crash)
   ```sh
   journalctl -u lavinmq | grep -A 50 "Unhandled exception"
   ```

6. **Core dump** (if available)
   ```sh
   coredumpctl info lavinmq > coredump_info.txt
   ```

### Reproduction Steps

Provide clear steps to reproduce the issue:

```markdown
1. Start LavinMQ with configuration X
2. Create queue with arguments Y
3. Publish N messages
4. Consume messages with prefetch Z
5. Observe error/behavior
```

### Expected vs Actual Behavior

Clearly state:
- What you expected to happen
- What actually happened
- Any error messages

### Additional Context

- Configuration files (`/etc/lavinmq/lavinmq.ini`)
- Queue/exchange definitions
- Client library and version
- Load characteristics (messages/sec, message sizes)
- Whether issue is reproducible

### Example Issue Template

```markdown
## Summary
Brief description of the issue

## Environment
- LavinMQ version: 2.0.0
- OS: Ubuntu 22.04
- Crystal version: 1.11.2

## Reproduction Steps
1. Step one
2. Step two
3. Step three

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Logs and Diagnostics
Attached:
- server_state.txt (USR1 dump)
- lavinmq_logs.txt (error logs)
- perf.data (performance recording, if applicable)

## Stack Trace
[paste stack trace here]

## Additional Context
Any other relevant information
```

## Common Issues and Quick Fixes

### LavinMQ Won't Start

```sh
# Check logs for errors
journalctl -u lavinmq --since "5 minutes ago"

# Check data directory lock
ls -la /var/lib/lavinmq/.lock

# Check permissions
ls -ld /var/lib/lavinmq
sudo chown -R lavinmq:lavinmq /var/lib/lavinmq

# Check disk space
df -h /var/lib/lavinmq
```

### High Memory Usage

```sh
# Dump state to see what's using memory
kill -USR1 $(pidof lavinmq)

# Force garbage collection
kill -USR2 $(pidof lavinmq)

# Wait a moment and check memory again
ps aux | grep lavinmq
```

### Connection Issues

```sh
# Verify LavinMQ is listening
sudo lsof -i :5672 -i :15672

# Check for connection errors in logs
journalctl -u lavinmq | grep -i "connection\|authentication"

# Check firewall
sudo iptables -L -n | grep 5672
```

### Slow Performance

```sh
# Check what's consuming CPU
sudo perf top -p $(pidof lavinmq)

# Check disk I/O
iostat -x 1

# Check for disk-related errors
journalctl -u lavinmq | grep -i "disk\|i/o"
```

### Queue Message Buildup

```sh
# Dump state to see consumer status
kill -USR1 $(pidof lavinmq)
journalctl -u lavinmq | grep -A 20 "Queue <queue-name>"

# Check if consumers are active and prefetch settings
# Check for unacked messages
```

## Advanced Debugging

### Finding Memory Leaks

```sh
# Compile with GC disabled (for testing only!)
make bin/lavinmq CRYSTAL_FLAGS=-Dgc_none

# Run with Valgrind massif
valgrind --tool=massif -- bin/lavinmq -D /tmp/test_data

# After running for a while, stop and analyze
ms_print massif.out.$(pidof lavinmq)

# Or use massif-visualizer for GUI
massif-visualizer massif.out.*
```

### Trace System Calls

```sh
# Trace system calls made by LavinMQ
sudo strace -p $(pidof lavinmq) -f -e trace=open,close,read,write

# Trace specific syscalls
sudo strace -p $(pidof lavinmq) -e trace=network

# Count syscalls
sudo perf trace --summary --pid $(pidof lavinmq)
```

### Network Packet Capture

```sh
# Capture AMQP traffic
sudo tcpdump -i any -w lavinmq_traffic.pcap port 5672

# Capture and display
sudo tcpdump -i any -A port 5672

# Analyze with Wireshark
wireshark lavinmq_traffic.pcap
```

## Getting Help

- **GitHub Issues**: https://github.com/cloudamqp/lavinmq/issues
- **Documentation**: https://lavinmq.com/documentation
- **Community**: Start discussions on GitHub, or join our [slack channel](https://join.slack.com/t/lavinmq/shared_invite/zt-1kzmlb85z-J_7QcOjMRyhECLTZ7Gprhg)
