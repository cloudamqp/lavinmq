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

```sh
# Follow logs in real-time
journalctl --follow --unit lavinmq

# View recent logs
journalctl --unit lavinmq --since "1 hour ago"

# Export logs to a file for sharing
journalctl --unit lavinmq --since today > lavinmq_logs.txt
```

### macOS

```sh
# If running as launchd service, check system logs
log stream --predicate 'process == "lavinmq"' --level debug

# Or run LavinMQ in foreground with output redirection
./lavinmq -D /path/to/data 2>&1 | tee lavinmq.log

# View existing log file
tail -f lavinmq.log
```

### Docker

```sh
# Follow logs in real-time
docker logs --follow <container_name>
```

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

### Using lsof (Linux and macOS)

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

### TCP Connection States (Linux)
```sh
# Show connection states
sudo ss -antp | grep $(pidof lavinmq)

# Count connections by state
sudo ss -antp | grep $(pidof lavinmq) | awk '{print $1}' | sort | uniq -c
```

### TCP Connection States (macOS)
```sh
# Show connection states
lsof -iTCP -a -c lavinmq

# Count connections by state
lsof -iTCP -a -c lavinmq | awk 'NR>1 {print $10}' | sort | uniq -c
```
## Performance Debugging

### Using perf top (Linux)

`perf top` shows real-time CPU usage by function, helping identify performance bottlenecks and infinite loops.

**Installation (not installed by default on Linux):**

```sh
sudo apt install linux-tools-common
sudo apt install linux-tools-aws  # or linux-tools-generic depending on kernel
```

**Usage:**

```sh
# Monitor LavinMQ CPU usage in real-time
sudo perf top --pid $(pidof lavinmq)
```

**What to look for:**
- Functions consuming high CPU percentage
- Tight loops or repeated function calls
- Unexpected hot paths

**Important:** If LavinMQ becomes unresponsive, take a screenshot of `perf top` output before restarting. This is crucial for debugging hangs and performance issues.

### Recording Performance Data (Linux)

For persistent performance issues, record data for later analysis:

```sh
# Record performance data (run for 30-60 seconds)
sudo perf record --call-graph lbr --pid $(pidof lavinmq)

# Stop recording with Ctrl+C after sufficient time

# View the report
sudo perf report

# The recording is saved to perf.data
ls -lh perf.data
```

**Send `perf.data` to developers** when reporting performance issues - it contains detailed call graph information.

## Core Dump Analysis

When LavinMQ crashes with a segmentation fault, a core dump captures the exact state of memory at crash time. LavinMQ allows the OS to generate core dumps, but **you need to enable core dump capture before reproducing the crash**.

### Linux

**If running LavinMQ manually:**

```sh
# Enable core dumps in your shell
ulimit -c unlimited

# Now run LavinMQ
./bin/lavinmq -D /path/to/data

# When it crashes, the core dump will be captured
```

**If running as a systemd service:**

```sh
# Create a systemd override to enable core dumps
sudo systemctl edit lavinmq

# Add this in the editor that opens:
[Service]
LimitCORE=infinity

# Save and exit, then reload and restart
sudo systemctl daemon-reload
sudo systemctl restart lavinmq
```

Modern Linux systems typically have `systemd-coredump` enabled by default, which automatically captures and manages core dumps.

**Checking for core dumps:**

```sh
# List recent core dumps
coredumpctl list

# Show info about the latest crash
coredumpctl info lavinmq
```

**Analyzing with GDB:**

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
```

**Exporting core dumps:**

```sh
# Save core dump to file for sharing
coredumpctl dump lavinmq -o lavinmq_core.dump

# Compress before sharing
gzip lavinmq_core.dump
```

### macOS

**Enabling core dumps:**

```sh
# Enable core dumps in your shell
ulimit -c unlimited

# Now run LavinMQ
./bin/lavinmq -D /path/to/data

# When it crashes, the core dump will be in /cores/
```

**Checking for core dumps:**

```sh
# Check for core dumps
ls -lh /cores/

# Check crash reports
ls -lh ~/Library/Logs/DiagnosticReports/lavinmq*
```

**Analyzing with LLDB:**

```sh
# Open core dump with LLDB
lldb lavinmq --core /cores/core.12345

# Essential commands:
# bt                 - Show backtrace
# bt all             - Show backtrace for all threads
# frame select N     - Switch to frame N
# thread list        - Show all threads
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

**Linux and macOS:**
```sh
# Check LavinMQ memory usage
ps aux | grep lavinmq
```

**Linux:**
```sh
# More detailed memory info
cat /proc/$(pidof lavinmq)/status | grep --ignore-case vmsize
cat /proc/$(pidof lavinmq)/status | grep --ignore-case vmrss

# Memory maps (can show memory leaks)
sudo pmap -x $(pidof lavinmq)
```

**macOS:**
```sh
# Detailed memory info
ps -o pid,rss,vsz -p $(pgrep lavinmq)

# Memory regions
sudo vmmap $(pgrep lavinmq)
```

### File Descriptors

LavinMQ uses file descriptors for client connections and temporarily during queue operations. High churn (rapid connection/queue creation and deletion) can cause temporary FD exhaustion, preventing new connections or queue operations.

**Linux:**
```sh
# Check FD limit
cat /proc/$(pidof lavinmq)/limits | grep "open files"

# Count currently open FDs
sudo ls -l /proc/$(pidof lavinmq)/fd | wc -l

# Show what FDs are open (with filenames and types)
sudo lsof -p $(pidof lavinmq)
```

**macOS:**
```sh
# Check FD limit
ulimit -n

# Show what FDs are open (with filenames and types)
sudo lsof -p $(pgrep lavinmq)

# Count currently open FDs
sudo lsof -p $(pgrep lavinmq) | wc -l
```

### Memory Map Areas

LavinMQ uses memory-mapped files for message storage (1 mmap per connection + 1 per message segment). With many connections or queues, you can hit the OS limit (default ~65k), preventing new connections or segment creation.

**Linux:**
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

### Trace System Calls (Linux)

```sh
# Trace file operations
sudo strace --attach $(pidof lavinmq) --follow-forks --trace=open,close,read,write,fstat,stat

# Trace memory-mapped file operations
sudo strace --attach $(pidof lavinmq) --follow-forks --trace=mmap,munmap,mremap,msync,ftruncate,truncate

# Trace network syscalls
sudo strace --attach $(pidof lavinmq) --trace=network

# Count syscalls
sudo perf trace --summary --pid $(pidof lavinmq)
```

### Trace System Calls (macOS)

```sh
# Trace all system calls
sudo dtruss -p $(pgrep lavinmq)

# Trace file operations only
sudo dtruss -t open,close,read,write,fstat -p $(pgrep lavinmq)

# Trace memory-mapped file operations
sudo dtruss -t mmap,munmap,msync,ftruncate -p $(pgrep lavinmq)
```

### Network Packet Capture

```sh
# Capture AMQP traffic
sudo tcpdump -i any -w lavinmq_traffic.pcap port 5672

# Capture and display
sudo tcpdump -i any -A port 5672

# Stream from remote machine to local Wireshark
ssh lavinmq.example.com sudo tcpdump -U -s0 'port 5672' -i ens5 -w - | wireshark -k -i -

# Analyze captured file with Wireshark
wireshark lavinmq_traffic.pcap
```

## Getting Help

- **GitHub Issues**: https://github.com/cloudamqp/lavinmq/issues
- **Documentation**: https://lavinmq.com/documentation
- **Community**: Start discussions on GitHub, or join our [slack channel](https://join.slack.com/t/lavinmq/shared_invite/zt-1kzmlb85z-J_7QcOjMRyhECLTZ7Gprhg)
