# LavinMQ Troubleshooting Guide

This guide helps you troubleshoot LavinMQ issues, gather debugging information, and create detailed bug reports for the LavinMQ open source repository.

## Quick Diagnostics

When experiencing issues with LavinMQ, start by gathering basic information:

```sh
# Check if LavinMQ is running
systemctl status lavinmq.service

# Check recent logs
journalctl --follow --unit lavinmq --lines 100

# Check LavinMQ version
lavinmq --version

# Check system resources
df -h /var/lib/lavinmq  # Disk space
free -h                  # Memory
```

## Common Issues and Quick Fixes

### LavinMQ Won't Start

```sh
# Check logs for errors
journalctl --unit lavinmq --since "5 minutes ago"

# Check permissions
ls -ld /var/lib/lavinmq
sudo chown -R lavinmq:lavinmq /var/lib/lavinmq

# Check disk space
df -h /var/lib/lavinmq
```

### Connection Issues

```sh
# Verify LavinMQ is listening
sudo lsof -i :5672 -i :15672

# Check for connection errors in logs
journalctl --unit lavinmq | grep --ignore-case "connection\|authentication"

# Check firewall (iptables on Linux)
sudo iptables -L -n | grep 5672

# Check firewall (nftables on modern Linux)
sudo nft list ruleset | grep 5672

# Check firewall (pf on macOS/BSD)
sudo pfctl -s rules | grep 5672
```

### Slow Performance

```sh
# Check what's consuming CPU
sudo perf top --pid $(pidof lavinmq)

# Check disk I/O
iostat -x 1

# Check for disk-related errors
journalctl --unit lavinmq | grep --ignore-case "disk\|i/o"
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

- **Garbage Collector statistics** - heap size, free/unmapped bytes, allocated memory, GC cycle counts, and reclaimed bytes (useful for identifying memory leaks or unusual growth patterns)
- **Fiber list** - all running fibers with their object IDs and names (useful for counting active fibers and detecting fiber leaks)

**Where to find the output:**

The output goes to `stdout`. If LavinMQ is managed by SystemD this is captured by `journald`:

```sh
kill -USR1 $(pidof lavinmq)
journalctl --pager-end --unit lavinmq
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

LavinMQ outputs all logs to `stdout`.

### Linux (systemd)

````sh
# Follow logs in real-time
journalctl --follow --unit lavinmq

### Docker

```sh
# Follow logs in real-time
docker logs --follow <container_name>
````

### Kubernetes

```sh
# Follow logs in real-time
kubectl logs --follow <pod_name>
```

### What to Look For

- **Exceptions and stack traces** - Crystal stack traces showing the error location
- **Connection errors** - Authentication failures, protocol errors
- **Disk errors** - I/O errors, disk full warnings
- **Memory pressure** - OOM warnings
- **Cluster issues** - Replication errors, leader election problems

## Network and Connection Debugging

Check LavinMQ's network connections to verify it's listening on the expected ports (5672 for AMQP, 15672 for HTTP API, 1883 for MQTT) and diagnose connection issues.

```sh
# Show LavinMQ's network connections and listening ports
sudo lsof -p $(pidof lavinmq) -i

# View connection states (Linux)
sudo ss -antp | grep $(pidof lavinmq)

# View connection states (macOS)
lsof -iTCP -a -c lavinmq
```

## Performance Debugging

If LavinMQ is consuming excessive CPU or becomes unresponsive, capture performance data to identify bottlenecks.

**Linux:**

```sh
# Monitor real-time CPU usage by function
sudo perf top --pid $(pidof lavinmq)

# Record performance data for later analysis (run for 30-60 seconds)
sudo perf record --call-graph lbr --pid $(pidof lavinmq)
sudo perf report  # View the recording
```

**Important:** If LavinMQ hangs or becomes unresponsive, capture a screenshot of `perf top` output before restarting. When reporting performance issues, include the `perf.data` file - it contains detailed call graph information.

## Core Dump Analysis

When LavinMQ crashes (segmentation fault), a core dump captures the memory state at crash time. Enable core dumps **before** reproducing the crash.

## System Resource Checks

### Disk Space

LavinMQ stores all messages on disk. A full disk will prevent LavinMQ from accepting new messages.

```sh
# Check disk space and usage
df -h /var/lib/lavinmq
du -sh /var/lib/lavinmq/*

# Find largest queues or message segments
du -sh /var/lib/lavinmq/*/* | sort -h | tail -20
```

### Memory

High memory usage may indicate a memory leak, too many unacked messages, or normal behavior under heavy load. Use USR1 signal (see above) to get detailed GC statistics.

```sh
# Check LavinMQ memory usage
ps aux | grep lavinmq

# More details (Linux)
cat /proc/$(pidof lavinmq)/status | grep -i vm

# More details (macOS)
ps -o pid,rss,vsz -p $(pgrep lavinmq)
```

### File Descriptors

LavinMQ uses file descriptors for client connections and temporarily during queue operations. High churn can cause FD exhaustion.

```sh
# Check FD limit and current usage (Linux)
cat /proc/$(pidof lavinmq)/limits | grep "open files"
sudo ls -l /proc/$(pidof lavinmq)/fd | wc -l

# Check FD limit and current usage (macOS)
ulimit -n
sudo lsof -p $(pgrep lavinmq) | wc -l
```

### Memory Map Areas (Linux)

LavinMQ uses memory-mapped files for message storage (1 mmap per connection + 1 per message segment). With many connections or queues, you can hit the OS limit (~65k default).

```sh
# Check current limit
cat /proc/sys/vm/max_map_count

# Increase limit if needed
sudo sysctl -w vm.max_map_count=1000000
```

## Cluster Debugging

For clustered LavinMQ instances:

```sh
# Check cluster status via API
# Replace 'username:password' with your actual credentials
curl -u username:password http://localhost:15672/api/cluster

# Check etcd ISR (In-Sync Replicas)
etcdctl get lavinmq/isr
```

## Debugging Tools

These tools are useful for diagnosing LavinMQ issues. Refer to each tool's documentation for detailed usage instructions.

- **lsof** - View LavinMQ's open files and network connections
- **ss / netstat** - Check TCP connection states and port bindings
- **perf** (Linux) - Profile CPU usage and identify performance bottlenecks
- **GDB** (Linux) - Analyze core dumps and backtraces from crashes
- **LLDB** (macOS) - Analyze core dumps and backtraces from crashes
- **strace** (Linux) / **dtruss** (macOS) - Trace system calls for low-level debugging
- **tcpdump** - Capture network packets for protocol-level debugging
- **Wireshark** - Analyze captured AMQP/MQTT traffic
