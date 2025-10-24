# LavinMQ Job Queue System - Technical Specification
## Language-Agnostic Job Processing with AMQP and Streams

---

## 1. EXECUTIVE SUMMARY

This specification outlines a complete AMQP-based job processing system built into LavinMQ that replicates Sidekiq's feature set while being language-agnostic and dependency-free. The system leverages LavinMQ's native capabilities, particularly **stream queues**, to provide:

- **Job enqueueing and execution** via pure AMQP clients
- **Retry logic** with exponential backoff stored in streams
- **Dead letter queue** for failed jobs using streams
- **Scheduled jobs** with time-based execution
- **Web UI** integrated into LavinMQ's management interface
- **Metrics collection** with historical statistics in streams
- **Queue prioritization** and weighted processing
- **Process heartbeats** tracked in streams

**Key Innovation**: All persistent data (retries, dead jobs, metrics, scheduled jobs, process heartbeats) are stored in **LavinMQ stream queues**, eliminating the need for external databases like PostgreSQL or Redis.

---

## 2. CORE ARCHITECTURE

### 2.1 LavinMQ Components (Backend)

```
LavinMQ Server
  ├─ Job Queue Manager
  │   ├─ Regular Job Queues (standard AMQP queues)
  │   ├─ Scheduled Job Store (stream queue + scheduler fiber)
  │   ├─ Retry Store (stream queue)
  │   ├─ Dead Letter Store (stream queue)
  │   ├─ Process Heartbeat Store (stream queue)
  │   └─ Metrics Store (stream queue)
  │
  ├─ Scheduler Fiber (5s poll interval)
  │   └─ Reads scheduled_jobs stream, publishes due jobs
  │
  ├─ HTTP Management API
  │   ├─ /api/job-queues (list, stats)
  │   ├─ /api/job-queues/:queue/jobs (list jobs)
  │   ├─ /api/scheduled-jobs (list, delete, trigger now)
  │   ├─ /api/retries (list, retry now, kill)
  │   ├─ /api/dead-jobs (list, retry, delete)
  │   ├─ /api/job-metrics (statistics)
  │   └─ /api/processes (worker process monitoring)
  │
  └─ Web UI
      ├─ Dashboard
      ├─ Queues view
      ├─ Scheduled jobs view
      ├─ Retries view
      ├─ Dead jobs view
      ├─ Metrics view
      └─ Processes view
```

### 2.2 Client/Worker Components (Language-Agnostic)

**Any programming language with an AMQP 0-9-1 client can:**

1. **Enqueue jobs**: Publish JSON messages to `jobs.direct` exchange
2. **Schedule jobs**: Publish to `scheduled_jobs` stream with timestamp
3. **Consume jobs**: Subscribe to job queues with prefetch
4. **Handle retries**: On failure, publish to `retries` stream
5. **Report heartbeats**: Publish to `heartbeats` stream every 10s
6. **Track metrics**: Publish to `job_metrics` stream

**Worker Thread Model** (client implementation):
```
Main Thread
  ├─ Heartbeat Thread (10s interval)
  └─ Worker Pool Manager
      ├─ Worker Thread 1 (AMQP channel, consumer)
      ├─ Worker Thread 2
      └─ Worker Thread N (configurable concurrency)
```

### 2.3 AMQP Topology

```
Exchanges:
  - jobs.direct          (direct, durable)   # Job routing
  - jobs.dead            (fanout, durable)   # Dead letter routing

Regular Queues (per logical queue):
  - queue.{name}         (durable, x-max-priority=10)

Stream Queues (infinite retention with filtering):
  - scheduled_jobs       (x-queue-type: stream) # Jobs to be scheduled
  - retries              (x-queue-type: stream) # Jobs awaiting retry
  - dead_jobs            (x-queue-type: stream) # Failed jobs
  - heartbeats           (x-queue-type: stream) # Worker process heartbeats
  - job_metrics          (x-queue-type: stream) # Execution metrics
```

---

## 3. LAVINMQ BACKEND FEATURES

### 3.1 Stream-Based Storage

**Why Streams?**
- **Append-only log**: Efficient writes, no deletions
- **Offset-based reading**: Fast filtering by timestamp
- **Consumer groups**: Multiple readers without interference
- **Retention policies**: Time-based or size-based limits
- **Durability**: Disk-backed persistence
- **Query support**: Stream filtering by timestamp, properties

**Stream Schema Conventions:**

All streams use JSON message bodies with common fields:
```json
{
  "type": "job|retry|dead|heartbeat|metric",
  "timestamp": 1234567890.0,
  "data": { ... }
}
```

### 3.2 Scheduled Jobs (LavinMQ Implementation)

**Stream Queue**: `scheduled_jobs`
- Type: `x-queue-type: stream`
- Retention: 30 days or 100GB (configurable)
- Consumer: LavinMQ scheduler fiber

**Message Format**:
```json
{
  "type": "scheduled_job",
  "jid": "unique-job-id",
  "scheduled_at": 1234567890.0,
  "job": {
    "class": "JobClassName",
    "args": [1, 2, "foo"],
    "queue": "default",
    "retry": 25,
    "enqueued_at": 1234567890.0,
    "tags": ["tag1"]
  }
}
```

**LavinMQ Scheduler Fiber**:
```crystal
# Runs every 5s
def schedule_jobs_fiber
  loop do
    sleep 5.seconds
    now = Time.utc.to_unix_f

    # Read scheduled_jobs stream with timestamp filter
    stream = @vhost.queues["scheduled_jobs"]
    stream.filter_by_timestamp(0, now).each do |msg|
      job = JSON.parse(msg.body)
      next if job["scheduled_at"].as_f > now

      # Publish to target queue
      publish_to_queue(job["job"]["queue"], job["job"])

      # Mark as processed (track offset)
      @last_scheduled_offset = msg.offset
    end
  end
end
```

**Client API** (any language):
```python
# Python example
import json, time, pika

connection = pika.BlockingConnection(pika.URLParameters('amqp://...'))
channel = connection.channel()

# Schedule job for 1 hour from now
scheduled_msg = {
    "type": "scheduled_job",
    "jid": str(uuid.uuid4()),
    "scheduled_at": time.time() + 3600,
    "job": {
        "class": "SendEmailJob",
        "args": ["user@example.com", "Welcome!"],
        "queue": "default",
        "retry": 25
    }
}

channel.basic_publish(
    exchange='',
    routing_key='scheduled_jobs',
    body=json.dumps(scheduled_msg),
    properties=pika.BasicProperties(delivery_mode=2)  # persistent
)
```

### 3.3 Retry System (LavinMQ Implementation)

**Stream Queue**: `retries`
- Type: `x-queue-type: stream`
- Retention: 30 days
- Consumer: LavinMQ retry scheduler fiber

**Message Format**:
```json
{
  "type": "retry",
  "jid": "job-id",
  "retry_at": 1234567890.0,
  "retry_count": 3,
  "failed_at": 1234567889.0,
  "error_message": "Connection timeout",
  "error_class": "TimeoutError",
  "job": {
    "class": "JobClassName",
    "args": [1, 2],
    "queue": "default",
    "retry": 25
  }
}
```

**LavinMQ Retry Scheduler Fiber**:
```crystal
def retry_jobs_fiber
  loop do
    sleep 5.seconds
    now = Time.utc.to_unix_f

    stream = @vhost.queues["retries"]
    stream.filter_by_timestamp(0, now).each do |msg|
      retry_entry = JSON.parse(msg.body)
      next if retry_entry["retry_at"].as_f > now

      # Re-publish to original queue
      job = retry_entry["job"]
      job["retry_count"] = retry_entry["retry_count"]
      publish_to_queue(job["queue"], job)

      @last_retry_offset = msg.offset
    end
  end
end
```

**Client Retry Logic** (worker implementation):
```python
# Python worker example
def process_job(channel, method, properties, body):
    try:
        job = json.loads(body)
        # Execute job
        execute_job_class(job['class'], job['args'])
        channel.basic_ack(method.delivery_tag)
    except Exception as e:
        retry_count = job.get('retry_count', 0)
        max_retries = job.get('retry', 25)

        if retry_count < max_retries:
            # Calculate backoff
            backoff = (retry_count ** 4) + 15 + random.randint(0, 30 * (retry_count + 1))

            # Publish to retries stream
            retry_msg = {
                "type": "retry",
                "jid": job['jid'],
                "retry_at": time.time() + backoff,
                "retry_count": retry_count + 1,
                "failed_at": time.time(),
                "error_message": str(e),
                "error_class": e.__class__.__name__,
                "job": job
            }
            channel.basic_publish('', 'retries', json.dumps(retry_msg))
            channel.basic_ack(method.delivery_tag)
        else:
            # Move to dead jobs
            publish_to_dead_queue(channel, job, e)
            channel.basic_ack(method.delivery_tag)
```

### 3.4 Dead Letter Queue (LavinMQ Implementation)

**Stream Queue**: `dead_jobs`
- Type: `x-queue-type: stream`
- Retention: 6 months or 10GB (configurable)
- Max messages: 10,000 (auto-trim oldest)

**Message Format**:
```json
{
  "type": "dead_job",
  "jid": "job-id",
  "died_at": 1234567890.0,
  "retry_count": 25,
  "error_message": "Database connection failed",
  "error_class": "ConnectionError",
  "backtrace": "line1\nline2\n...",
  "job": {
    "class": "ImportUserJob",
    "args": [12345],
    "queue": "default"
  }
}
```

**LavinMQ HTTP API** for dead jobs:
```crystal
# GET /api/dead-jobs?limit=100&offset=0
get "/api/dead-jobs" do |context, params|
  stream = @vhost.queues["dead_jobs"]
  limit = context.request.query_params["limit"]?.try(&.to_i) || 100
  offset = context.request.query_params["offset"]?.try(&.to_i64) || 0_i64

  messages = stream.read_from_offset(offset, limit)
  jobs = messages.map { |msg| JSON.parse(msg.body) }

  context.response.print(jobs.to_json)
end

# POST /api/dead-jobs/:jid/retry
post "/api/dead-jobs/:jid/retry" do |context, params|
  jid = params["jid"]
  stream = @vhost.queues["dead_jobs"]

  # Find job by jid (scan stream)
  dead_job = stream.find { |msg| JSON.parse(msg.body)["jid"] == jid }
  return 404 if dead_job.nil?

  # Re-publish to original queue
  job_data = JSON.parse(dead_job.body)["job"]
  job_data["retry_count"] = 0
  publish_to_queue(job_data["queue"], job_data)

  context.response.status_code = 204
end

# DELETE /api/dead-jobs/:jid
delete "/api/dead-jobs/:jid" do |context, params|
  # Note: Streams are append-only, so we maintain a "deleted_dead_jobs" set
  # in memory or use message properties to mark as deleted
  jid = params["jid"]
  @deleted_dead_jobs.add(jid)
  context.response.status_code = 204
end
```

### 3.5 Process Heartbeats (LavinMQ Implementation)

**Stream Queue**: `heartbeats`
- Type: `x-queue-type: stream`
- Retention: 5 minutes (rolling window)
- Purpose: Track active worker processes

**Message Format**:
```json
{
  "type": "heartbeat",
  "identity": "hostname:pid:tag",
  "hostname": "worker-01.example.com",
  "pid": 12345,
  "tag": "production",
  "concurrency": 10,
  "busy": 3,
  "beat": 1234567890.0,
  "rss_kb": 204800,
  "queues": ["critical", "default", "low"],
  "version": "1.0.0"
}
```

**LavinMQ Process Tracker**:
```crystal
class ProcessTracker
  def initialize(@vhost : VHost)
    @processes = Hash(String, ProcessInfo).new
    @heartbeat_stream = @vhost.queues["heartbeats"]
    spawn update_processes_loop
  end

  private def update_processes_loop
    last_offset = 0_i64
    loop do
      sleep 2.seconds

      # Read new heartbeats from stream
      @heartbeat_stream.read_from_offset(last_offset).each do |msg|
        heartbeat = JSON.parse(msg.body)
        identity = heartbeat["identity"].as_s

        @processes[identity] = ProcessInfo.from_json(heartbeat)
        last_offset = msg.offset
      end

      # Remove stale processes (no heartbeat in 60s)
      cutoff = Time.utc.to_unix_f - 60
      @processes.reject! { |_, proc| proc.beat < cutoff }
    end
  end

  def active_processes : Array(ProcessInfo)
    @processes.values
  end

  def total_busy_workers : Int32
    @processes.values.sum(&.busy)
  end
end
```

**Client Heartbeat** (worker implementation):
```ruby
# Ruby example
Thread.new do
  loop do
    sleep 10

    heartbeat = {
      type: "heartbeat",
      identity: "#{hostname}:#{Process.pid}:#{tag}",
      hostname: Socket.gethostname,
      pid: Process.pid,
      tag: "production",
      concurrency: worker_count,
      busy: busy_worker_count,
      beat: Time.now.to_f,
      rss_kb: `ps -o rss= -p #{Process.pid}`.to_i,
      queues: ["critical", "default"],
      version: "1.0.0"
    }

    channel.basic_publish('', 'heartbeats', heartbeat.to_json)
  end
end
```

### 3.6 Metrics Collection (LavinMQ Implementation)

**Stream Queue**: `job_metrics`
- Type: `x-queue-type: stream`
- Retention: 5 years
- Purpose: Per-job execution statistics

**Message Format**:
```json
{
  "type": "job_metric",
  "timestamp": 1234567890.0,
  "job_class": "SendEmailJob",
  "queue": "default",
  "duration_ms": 1234,
  "status": "success",
  "worker_identity": "worker-01:1234:prod"
}
```

**LavinMQ Metrics Aggregator**:
```crystal
class MetricsAggregator
  def initialize(@vhost : VHost)
    @metrics_stream = @vhost.queues["job_metrics"]
    @buckets = Hash(String, MetricBucket).new
    spawn aggregate_metrics_loop
  end

  private def aggregate_metrics_loop
    last_offset = 0_i64
    loop do
      sleep 10.seconds

      @metrics_stream.read_from_offset(last_offset, limit: 1000).each do |msg|
        metric = JSON.parse(msg.body)
        job_class = metric["job_class"].as_s
        timestamp = metric["timestamp"].as_f

        # Bucket by job_class and 1-minute window
        bucket_key = "#{job_class}:#{(timestamp / 60).to_i}"
        bucket = @buckets[bucket_key] ||= MetricBucket.new

        bucket.count += 1
        bucket.total_ms += metric["duration_ms"].as_i
        bucket.success += 1 if metric["status"] == "success"
        bucket.failed += 1 if metric["status"] == "failed"

        last_offset = msg.offset
      end
    end
  end

  def stats_for_job(job_class : String, period : Time::Span) : JobStats
    # Query aggregated buckets
    cutoff = Time.utc - period
    relevant_buckets = @buckets.select do |key, _|
      key.starts_with?(job_class) && bucket_timestamp(key) > cutoff
    end

    JobStats.new(
      total: relevant_buckets.sum(&.count),
      avg_ms: relevant_buckets.sum(&.total_ms) / relevant_buckets.sum(&.count),
      success_rate: relevant_buckets.sum(&.success) / relevant_buckets.sum(&.count).to_f
    )
  end
end
```

**Client Metrics Reporting** (worker implementation):
```go
// Go example
func reportMetric(channel *amqp.Channel, jobClass string, durationMs int, status string) {
    metric := map[string]interface{}{
        "type":            "job_metric",
        "timestamp":       float64(time.Now().Unix()),
        "job_class":       jobClass,
        "queue":           "default",
        "duration_ms":     durationMs,
        "status":          status,
        "worker_identity": fmt.Sprintf("%s:%d:prod", hostname, os.Getpid()),
    }

    body, _ := json.Marshal(metric)
    channel.Publish("", "job_metrics", false, false, amqp.Publishing{
        DeliveryMode: amqp.Persistent,
        ContentType:  "application/json",
        Body:         body,
    })
}

func processJob(delivery amqp.Delivery) {
    start := time.Now()
    var status string

    defer func() {
        duration := int(time.Since(start).Milliseconds())
        reportMetric(channel, job.Class, duration, status)
    }()

    err := executeJob(delivery.Body)
    if err != nil {
        status = "failed"
    } else {
        status = "success"
    }
}
```

### 3.7 Web UI (LavinMQ Integration)

**Routes** (added to LavinMQ HTTP server):

```
/job-queues              → Dashboard
/job-queues/:queue       → Queue detail view
/scheduled-jobs          → Scheduled jobs management
/retries                 → Retry queue management
/dead-jobs               → Dead letter queue management
/job-metrics             → Metrics and statistics
/worker-processes        → Active worker monitoring
```

**Dashboard View** (`/job-queues`):
- Total jobs processed (from metrics stream)
- Failed jobs count (from metrics stream)
- Active queues with sizes
- Scheduled jobs count (from scheduled_jobs stream)
- Retry queue size (from retries stream)
- Dead queue size (from dead_jobs stream)
- Active worker processes (from heartbeats stream)
- Busy workers (from heartbeats stream)
- Historical chart (last 30 days from metrics)

**Implementation Example**:
```crystal
# /src/lavinmq/http/controller/job_queues.cr
class JobQueuesController < Controller
  get "/job-queues" do |context, _|
    stats = {
      processed_today: @job_tracker.processed_count(Time.utc.at_beginning_of_day),
      failed_today: @job_tracker.failed_count(Time.utc.at_beginning_of_day),
      queues: @vhost.queues.reject { |k, _| k.in?("scheduled_jobs", "retries", "dead_jobs", "heartbeats", "job_metrics") },
      scheduled_count: count_stream_messages("scheduled_jobs"),
      retry_count: count_stream_messages("retries"),
      dead_count: count_stream_messages("dead_jobs"),
      active_processes: @process_tracker.active_processes.size,
      busy_workers: @process_tracker.total_busy_workers
    }

    render_json(context, stats)
  end

  get "/api/scheduled-jobs" do |context, _|
    limit = context.request.query_params["limit"]?.try(&.to_i) || 100
    offset = context.request.query_params["offset"]?.try(&.to_i64) || 0_i64

    stream = @vhost.queues["scheduled_jobs"]
    messages = stream.read_from_offset(offset, limit)

    jobs = messages.map { |msg| JSON.parse(msg.body) }
    render_json(context, jobs)
  end

  post "/api/scheduled-jobs/:jid/trigger" do |context, params|
    jid = params["jid"]
    stream = @vhost.queues["scheduled_jobs"]

    # Find and trigger job immediately
    scheduled_job = stream.find { |msg| JSON.parse(msg.body)["jid"] == jid }
    return 404 if scheduled_job.nil?

    job_data = JSON.parse(scheduled_job.body)["job"]
    publish_to_queue(job_data["queue"], job_data)

    context.response.status_code = 204
  end
end
```

---

## 4. CLIENT/WORKER IMPLEMENTATION GUIDE

### 4.1 Language-Agnostic Job Format

**Standard Job Message** (published to `queue.{name}`):
```json
{
  "jid": "b4a577edbccf1d805744efa597",
  "class": "JobClassName",
  "args": [1, "foo", {"key": "value"}],
  "queue": "default",
  "retry": 25,
  "retry_count": 0,
  "enqueued_at": 1234567890.0,
  "backtrace": 15,
  "tags": ["urgent", "user_facing"]
}
```

### 4.2 Job Enqueueing (Producer)

**Python Example**:
```python
import pika, json, time, uuid

class JobClient:
    def __init__(self, amqp_url):
        self.connection = pika.BlockingConnection(pika.URLParameters(amqp_url))
        self.channel = self.connection.channel()

    def enqueue(self, job_class, args, queue="default", retry=25):
        job = {
            "jid": uuid.uuid4().hex,
            "class": job_class,
            "args": args,
            "queue": queue,
            "retry": retry,
            "retry_count": 0,
            "enqueued_at": time.time()
        }

        self.channel.basic_publish(
            exchange='jobs.direct',
            routing_key=queue,
            body=json.dumps(job),
            properties=pika.BasicProperties(delivery_mode=2)
        )

        return job['jid']

    def schedule(self, job_class, args, scheduled_at, queue="default"):
        scheduled_job = {
            "type": "scheduled_job",
            "jid": uuid.uuid4().hex,
            "scheduled_at": scheduled_at,
            "job": {
                "class": job_class,
                "args": args,
                "queue": queue,
                "retry": 25,
                "enqueued_at": time.time()
            }
        }

        self.channel.basic_publish(
            exchange='',
            routing_key='scheduled_jobs',
            body=json.dumps(scheduled_job),
            properties=pika.BasicProperties(delivery_mode=2)
        )

# Usage
client = JobClient("amqp://localhost")
client.enqueue("SendEmailJob", ["user@example.com", "Hello!"])
client.schedule("SendReminderJob", [123], time.time() + 3600)
```

**JavaScript Example**:
```javascript
const amqp = require('amqplib');

class JobClient {
  constructor(amqpUrl) {
    this.connection = null;
    this.channel = null;
    this.amqpUrl = amqpUrl;
  }

  async connect() {
    this.connection = await amqp.connect(this.amqpUrl);
    this.channel = await this.connection.createChannel();
  }

  async enqueue(jobClass, args, queue = 'default', retry = 25) {
    const job = {
      jid: crypto.randomUUID().replace(/-/g, ''),
      class: jobClass,
      args: args,
      queue: queue,
      retry: retry,
      retry_count: 0,
      enqueued_at: Date.now() / 1000
    };

    await this.channel.publish(
      'jobs.direct',
      queue,
      Buffer.from(JSON.stringify(job)),
      { persistent: true }
    );

    return job.jid;
  }

  async schedule(jobClass, args, scheduledAt, queue = 'default') {
    const scheduledJob = {
      type: 'scheduled_job',
      jid: crypto.randomUUID().replace(/-/g, ''),
      scheduled_at: scheduledAt,
      job: {
        class: jobClass,
        args: args,
        queue: queue,
        retry: 25,
        enqueued_at: Date.now() / 1000
      }
    };

    await this.channel.sendToQueue(
      'scheduled_jobs',
      Buffer.from(JSON.stringify(scheduledJob)),
      { persistent: true }
    );
  }
}

// Usage
const client = new JobClient('amqp://localhost');
await client.connect();
await client.enqueue('SendEmailJob', ['user@example.com', 'Hello!']);
await client.schedule('SendReminderJob', [123], Date.now() / 1000 + 3600);
```

### 4.3 Job Processing (Worker)

**Ruby Example**:
```ruby
require 'bunny'
require 'json'

class Worker
  def initialize(amqp_url, queues, concurrency: 5)
    @connection = Bunny.new(amqp_url)
    @connection.start
    @queues = queues
    @concurrency = concurrency
    @job_registry = {}
  end

  def register_job(job_class_name, &block)
    @job_registry[job_class_name] = block
  end

  def start
    # Spawn heartbeat thread
    Thread.new { heartbeat_loop }

    # Spawn worker threads
    @concurrency.times.map do |i|
      Thread.new { worker_loop(i) }
    end.each(&:join)
  end

  private

  def worker_loop(worker_id)
    channel = @connection.create_channel
    channel.prefetch(1)

    @queues.each do |queue_name|
      queue = channel.queue(queue_name, durable: true)

      queue.subscribe(manual_ack: true) do |delivery_info, properties, body|
        job = JSON.parse(body)
        start_time = Time.now

        begin
          # Execute job
          job_handler = @job_registry[job['class']]
          raise "Unknown job class: #{job['class']}" unless job_handler

          job_handler.call(*job['args'])

          # Success - ack and report metric
          channel.ack(delivery_info.delivery_tag)
          report_metric(job, (Time.now - start_time) * 1000, 'success')

        rescue => e
          # Handle retry or dead letter
          handle_job_failure(channel, job, e, delivery_info)
        end
      end
    end

    sleep # Keep thread alive
  end

  def handle_job_failure(channel, job, error, delivery_info)
    retry_count = job['retry_count'] || 0
    max_retries = job['retry'] || 25

    if retry_count < max_retries
      # Calculate backoff
      backoff = (retry_count ** 4) + 15 + rand(30 * (retry_count + 1))

      # Publish to retries stream
      retry_msg = {
        type: 'retry',
        jid: job['jid'],
        retry_at: Time.now.to_f + backoff,
        retry_count: retry_count + 1,
        failed_at: Time.now.to_f,
        error_message: error.message,
        error_class: error.class.name,
        job: job
      }

      channel.default_exchange.publish(
        retry_msg.to_json,
        routing_key: 'retries',
        persistent: true
      )
    else
      # Publish to dead_jobs stream
      dead_msg = {
        type: 'dead_job',
        jid: job['jid'],
        died_at: Time.now.to_f,
        retry_count: retry_count,
        error_message: error.message,
        error_class: error.class.name,
        backtrace: error.backtrace.join("\n"),
        job: job
      }

      channel.default_exchange.publish(
        dead_msg.to_json,
        routing_key: 'dead_jobs',
        persistent: true
      )
    end

    # Ack original message
    channel.ack(delivery_info.delivery_tag)
    report_metric(job, 0, 'failed')
  end

  def report_metric(job, duration_ms, status)
    metric = {
      type: 'job_metric',
      timestamp: Time.now.to_f,
      job_class: job['class'],
      queue: job['queue'],
      duration_ms: duration_ms.to_i,
      status: status,
      worker_identity: "#{Socket.gethostname}:#{Process.pid}:prod"
    }

    channel = @connection.create_channel
    channel.default_exchange.publish(
      metric.to_json,
      routing_key: 'job_metrics',
      persistent: true
    )
  end

  def heartbeat_loop
    loop do
      sleep 10

      heartbeat = {
        type: 'heartbeat',
        identity: "#{Socket.gethostname}:#{Process.pid}:prod",
        hostname: Socket.gethostname,
        pid: Process.pid,
        tag: 'production',
        concurrency: @concurrency,
        busy: @busy_count || 0,
        beat: Time.now.to_f,
        rss_kb: `ps -o rss= -p #{Process.pid}`.to_i,
        queues: @queues,
        version: '1.0.0'
      }

      channel = @connection.create_channel
      channel.default_exchange.publish(
        heartbeat.to_json,
        routing_key: 'heartbeats',
        persistent: true
      )
    end
  end
end

# Usage
worker = Worker.new('amqp://localhost', ['critical', 'default'], concurrency: 10)

worker.register_job('SendEmailJob') do |email, message|
  # Send email logic
  puts "Sending email to #{email}: #{message}"
end

worker.register_job('ProcessImageJob') do |image_id|
  # Image processing logic
  puts "Processing image #{image_id}"
end

worker.start
```

### 4.4 Go Worker Example

```go
package main

import (
    "encoding/json"
    "fmt"
    "log"
    "math"
    "math/rand"
    "os"
    "time"

    "github.com/streadway/amqp"
)

type Job struct {
    JID         string        `json:"jid"`
    Class       string        `json:"class"`
    Args        []interface{} `json:"args"`
    Queue       string        `json:"queue"`
    Retry       int           `json:"retry"`
    RetryCount  int           `json:"retry_count"`
    EnqueuedAt  float64       `json:"enqueued_at"`
}

type Worker struct {
    connection  *amqp.Connection
    queues      []string
    concurrency int
    jobHandlers map[string]func([]interface{}) error
}

func NewWorker(amqpURL string, queues []string, concurrency int) (*Worker, error) {
    conn, err := amqp.Dial(amqpURL)
    if err != nil {
        return nil, err
    }

    return &Worker{
        connection:  conn,
        queues:      queues,
        concurrency: concurrency,
        jobHandlers: make(map[string]func([]interface{}) error),
    }, nil
}

func (w *Worker) RegisterJob(jobClass string, handler func([]interface{}) error) {
    w.jobHandlers[jobClass] = handler
}

func (w *Worker) Start() error {
    // Start heartbeat goroutine
    go w.heartbeatLoop()

    // Start worker goroutines
    for i := 0; i < w.concurrency; i++ {
        go w.workerLoop(i)
    }

    select {} // Block forever
}

func (w *Worker) workerLoop(workerID int) {
    ch, err := w.connection.Channel()
    if err != nil {
        log.Fatalf("Worker %d: Failed to open channel: %v", workerID, err)
    }
    defer ch.Close()

    ch.Qos(1, 0, false)

    for _, queueName := range w.queues {
        msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
        if err != nil {
            log.Printf("Worker %d: Failed to consume from %s: %v", workerID, queueName, err)
            continue
        }

        go func(deliveries <-chan amqp.Delivery) {
            for msg := range deliveries {
                w.processJob(ch, msg)
            }
        }(msgs)
    }

    select {} // Keep goroutine alive
}

func (w *Worker) processJob(ch *amqp.Channel, delivery amqp.Delivery) {
    var job Job
    err := json.Unmarshal(delivery.Body, &job)
    if err != nil {
        log.Printf("Failed to parse job: %v", err)
        delivery.Ack(false)
        return
    }

    start := time.Now()
    handler, exists := w.jobHandlers[job.Class]

    if !exists {
        log.Printf("Unknown job class: %s", job.Class)
        delivery.Nack(false, false)
        return
    }

    err = handler(job.Args)
    duration := time.Since(start)

    if err != nil {
        w.handleJobFailure(ch, job, err)
        w.reportMetric(job, int(duration.Milliseconds()), "failed")
    } else {
        delivery.Ack(false)
        w.reportMetric(job, int(duration.Milliseconds()), "success")
    }
}

func (w *Worker) handleJobFailure(ch *amqp.Channel, job Job, err error) {
    if job.RetryCount < job.Retry {
        // Calculate backoff
        backoff := math.Pow(float64(job.RetryCount), 4) + 15 + float64(rand.Intn(30*(job.RetryCount+1)))

        retryMsg := map[string]interface{}{
            "type":          "retry",
            "jid":           job.JID,
            "retry_at":      float64(time.Now().Unix()) + backoff,
            "retry_count":   job.RetryCount + 1,
            "failed_at":     float64(time.Now().Unix()),
            "error_message": err.Error(),
            "error_class":   fmt.Sprintf("%T", err),
            "job":           job,
        }

        body, _ := json.Marshal(retryMsg)
        ch.Publish("", "retries", false, false, amqp.Publishing{
            DeliveryMode: amqp.Persistent,
            ContentType:  "application/json",
            Body:         body,
        })
    } else {
        // Move to dead jobs
        deadMsg := map[string]interface{}{
            "type":          "dead_job",
            "jid":           job.JID,
            "died_at":       float64(time.Now().Unix()),
            "retry_count":   job.RetryCount,
            "error_message": err.Error(),
            "error_class":   fmt.Sprintf("%T", err),
            "job":           job,
        }

        body, _ := json.Marshal(deadMsg)
        ch.Publish("", "dead_jobs", false, false, amqp.Publishing{
            DeliveryMode: amqp.Persistent,
            ContentType:  "application/json",
            Body:         body,
        })
    }
}

func (w *Worker) reportMetric(job Job, durationMs int, status string) {
    metric := map[string]interface{}{
        "type":            "job_metric",
        "timestamp":       float64(time.Now().Unix()),
        "job_class":       job.Class,
        "queue":           job.Queue,
        "duration_ms":     durationMs,
        "status":          status,
        "worker_identity": fmt.Sprintf("%s:%d:prod", os.Hostname(), os.Getpid()),
    }

    ch, _ := w.connection.Channel()
    defer ch.Close()

    body, _ := json.Marshal(metric)
    ch.Publish("", "job_metrics", false, false, amqp.Publishing{
        DeliveryMode: amqp.Persistent,
        ContentType:  "application/json",
        Body:         body,
    })
}

func (w *Worker) heartbeatLoop() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        hostname, _ := os.Hostname()
        heartbeat := map[string]interface{}{
            "type":        "heartbeat",
            "identity":    fmt.Sprintf("%s:%d:prod", hostname, os.Getpid()),
            "hostname":    hostname,
            "pid":         os.Getpid(),
            "tag":         "production",
            "concurrency": w.concurrency,
            "busy":        0, // Track this properly
            "beat":        float64(time.Now().Unix()),
            "queues":      w.queues,
            "version":     "1.0.0",
        }

        ch, _ := w.connection.Channel()
        body, _ := json.Marshal(heartbeat)
        ch.Publish("", "heartbeats", false, false, amqp.Publishing{
            DeliveryMode: amqp.Persistent,
            ContentType:  "application/json",
            Body:         body,
        })
        ch.Close()
    }
}

func main() {
    worker, err := NewWorker("amqp://localhost", []string{"critical", "default"}, 10)
    if err != nil {
        log.Fatal(err)
    }

    worker.RegisterJob("SendEmailJob", func(args []interface{}) error {
        email := args[0].(string)
        message := args[1].(string)
        fmt.Printf("Sending email to %s: %s\n", email, message)
        return nil
    })

    worker.RegisterJob("ProcessImageJob", func(args []interface{}) error {
        imageID := int(args[0].(float64))
        fmt.Printf("Processing image %d\n", imageID)
        return nil
    })

    log.Println("Worker started")
    worker.Start()
}
```

---

## 5. LAVINMQ IMPLEMENTATION SUMMARY

### 5.1 New Files to Create

```
src/lavinmq/job_queue/
  ├─ job_tracker.cr              # Manages all job queue streams
  ├─ scheduled_job_scheduler.cr  # Fiber that polls scheduled_jobs stream
  ├─ retry_scheduler.cr          # Fiber that polls retries stream
  ├─ process_tracker.cr          # Tracks worker processes from heartbeats
  └─ metrics_aggregator.cr       # Aggregates job_metrics stream

src/lavinmq/http/controller/
  └─ job_queues_controller.cr    # HTTP API for job queue management

views/
  ├─ job-queues.ecr              # Dashboard view
  ├─ scheduled-jobs.ecr          # Scheduled jobs view
  ├─ retries.ecr                 # Retry queue view
  ├─ dead-jobs.ecr               # Dead jobs view
  └─ job-metrics.ecr             # Metrics view

static/js/
  ├─ job-queues.js               # Dashboard controller
  ├─ scheduled-jobs.js           # Scheduled jobs controller
  ├─ retries.js                  # Retries controller
  ├─ dead-jobs.js                # Dead jobs controller
  └─ job-metrics.js              # Metrics controller
```

### 5.2 Stream Queue Initialization

**In VHost initialization**:
```crystal
def initialize_job_queues
  # Create stream queues if they don't exist
  declare_stream_queue("scheduled_jobs", max_age: 30.days)
  declare_stream_queue("retries", max_age: 30.days)
  declare_stream_queue("dead_jobs", max_age: 180.days, max_length: 10_000)
  declare_stream_queue("heartbeats", max_age: 5.minutes)
  declare_stream_queue("job_metrics", max_age: 5.years)

  # Initialize job queue components
  @job_tracker = JobQueue::JobTracker.new(self)
  @scheduled_job_scheduler = JobQueue::ScheduledJobScheduler.new(self)
  @retry_scheduler = JobQueue::RetryScheduler.new(self)
  @process_tracker = JobQueue::ProcessTracker.new(self)
  @metrics_aggregator = JobQueue::MetricsAggregator.new(self)

  # Start scheduler fibers
  @scheduled_job_scheduler.start
  @retry_scheduler.start
end

private def declare_stream_queue(name : String, max_age : Time::Span? = nil, max_length : Int32? = nil)
  return if @queues.has_key?(name)

  arguments = {
    "x-queue-type" => "stream"
  }
  arguments["x-max-age"] = "#{max_age.total_seconds.to_i}s" if max_age
  arguments["x-max-length"] = max_length if max_length

  queue = Queue.new(self, name, true, false, arguments)
  @queues[name] = queue
end
```

### 5.3 Key Implementation Classes

**JobTracker** (`job_tracker.cr`):
```crystal
module LavinMQ::JobQueue
  class JobTracker
    def initialize(@vhost : VHost)
      @scheduled_jobs_stream = @vhost.queues["scheduled_jobs"]
      @retries_stream = @vhost.queues["retries"]
      @dead_jobs_stream = @vhost.queues["dead_jobs"]
      @metrics_stream = @vhost.queues["job_metrics"]
    end

    def scheduled_jobs(limit : Int32 = 100, offset : Int64 = 0) : Array(ScheduledJob)
      @scheduled_jobs_stream.read_from_offset(offset, limit).map do |msg|
        ScheduledJob.from_json(msg.body)
      end
    end

    def retry_jobs(limit : Int32 = 100, offset : Int64 = 0) : Array(RetryJob)
      @retries_stream.read_from_offset(offset, limit).map do |msg|
        RetryJob.from_json(msg.body)
      end
    end

    def dead_jobs(limit : Int32 = 100, offset : Int64 = 0) : Array(DeadJob)
      @dead_jobs_stream.read_from_offset(offset, limit).map do |msg|
        DeadJob.from_json(msg.body)
      end
    end

    def find_scheduled_job(jid : String) : ScheduledJob?
      # Scan stream for matching jid
      @scheduled_jobs_stream.each do |msg|
        job = ScheduledJob.from_json(msg.body)
        return job if job.jid == jid
      end
      nil
    end

    def trigger_scheduled_job(jid : String) : Bool
      job = find_scheduled_job(jid)
      return false unless job

      # Publish to target queue
      msg = Message.new(
        exchange_name: "jobs.direct",
        routing_key: job.job.queue,
        body: job.job.to_json
      )
      @vhost.publish(msg)
      true
    end
  end
end
```

**ScheduledJobScheduler** (`scheduled_job_scheduler.cr`):
```crystal
module LavinMQ::JobQueue
  class ScheduledJobScheduler
    def initialize(@vhost : VHost)
      @stream = @vhost.queues["scheduled_jobs"]
      @last_offset = 0_i64
      @closed = Channel(Nil).new
    end

    def start
      spawn do
        loop do
          select
          when @closed.receive?
            break
          when timeout(5.seconds)
            process_scheduled_jobs
          end
        end
      end
    end

    def close
      @closed.close
    end

    private def process_scheduled_jobs
      now = Time.utc.to_unix_f

      @stream.read_from_offset(@last_offset, limit: 100).each do |msg|
        scheduled_job = JSON.parse(msg.body)
        scheduled_at = scheduled_job["scheduled_at"].as_f

        if scheduled_at <= now
          # Publish to target queue
          job_data = scheduled_job["job"]
          message = Message.new(
            exchange_name: "jobs.direct",
            routing_key: job_data["queue"].as_s,
            body: job_data.to_json
          )
          @vhost.publish(message)

          Log.info { "Scheduled job #{scheduled_job["jid"]} published to queue #{job_data["queue"]}" }
        end

        @last_offset = msg.offset
      end
    end
  end
end
```

---

## 6. FEATURE COMPARISON

| Feature | Sidekiq (Redis) | LavinMQ Job Queue (Streams) |
|---------|----------------|----------------------------|
| Job enqueueing | ✅ RPUSH to list | ✅ Publish to AMQP queue |
| Scheduled jobs | ✅ ZADD to sorted set | ✅ Stream with timestamp filtering |
| Retries | ✅ ZADD to sorted set | ✅ Stream with retry_at timestamp |
| Dead letter queue | ✅ ZADD to sorted set | ✅ Stream (append-only) |
| Metrics | ✅ Redis strings/hashes | ✅ Stream with aggregation |
| Process heartbeats | ✅ Redis hash with TTL | ✅ Stream with time-based filtering |
| Web UI | ✅ Sinatra/Rack | ✅ Integrated LavinMQ UI |
| Language support | ❌ Ruby only | ✅ Any AMQP client (universal) |
| External dependencies | ❌ Requires Redis | ✅ None (all in LavinMQ) |
| Durability | ⚠️ Optional (RDB/AOF) | ✅ Native (disk-backed streams) |
| Multi-datacenter | ❌ Redis replication | ✅ AMQP federation |
| Query performance | ✅ O(log n) sorted sets | ⚠️ O(n) stream scanning |
| Storage efficiency | ✅ Memory-optimized | ✅ Disk-backed, compressible |

---

## 7. PERFORMANCE TARGETS

- **Throughput**: 10,000+ jobs/second per LavinMQ instance
- **Latency**: <10ms enqueue, <15ms dequeue
- **Stream read**: <5ms for offset-based reads (100 messages)
- **Scheduler latency**: <5s for scheduled job execution (poll interval)
- **Memory**: <500 MB overhead for job queue features
- **Disk**: ~100 GB/year for 1M jobs/day with full metrics

---

## 8. MIGRATION FROM SIDEKIQ

### 8.1 Client Migration

**Before** (Sidekiq):
```ruby
class SendEmailJob
  include Sidekiq::Job
  sidekiq_options queue: 'critical', retry: 10

  def perform(email, message)
    # Send email
  end
end

SendEmailJob.perform_async('user@example.com', 'Hello')
```

**After** (LavinMQ Job Queue):
```ruby
require 'bunny'

class JobClient
  def self.connection
    @connection ||= Bunny.new('amqp://localhost').tap(&:start)
  end

  def self.enqueue(job_class, args, queue: 'default', retry: 25)
    channel = connection.create_channel
    job = {
      jid: SecureRandom.hex(12),
      class: job_class,
      args: args,
      queue: queue,
      retry: retry,
      retry_count: 0,
      enqueued_at: Time.now.to_f
    }

    channel.default_exchange.publish(
      job.to_json,
      routing_key: queue,
      persistent: true
    )
  end
end

# New usage
JobClient.enqueue('SendEmailJob', ['user@example.com', 'Hello'], queue: 'critical', retry: 10)
```

### 8.2 Worker Migration

Create a generic AMQP worker (see section 4.3) and register job handlers.

---

## 9. CONCLUSION

This specification provides a complete blueprint for building a **language-agnostic job processing system** into LavinMQ using **stream queues** for all persistent storage. Key advantages:

1. **Zero External Dependencies**: No Redis, PostgreSQL, or other databases needed
2. **Universal Language Support**: Any AMQP client can enqueue and process jobs
3. **Native Durability**: Stream queues provide disk-backed persistence
4. **Integrated Management**: Web UI built into LavinMQ's existing interface
5. **Scalable Architecture**: Horizontal scaling via AMQP clustering and federation
6. **Sidekiq-Compatible**: Similar feature set and API design

**Estimated Development Time**: 3-4 months (2 engineers)

**Key Success Metrics**:
- 10,000+ jobs/second throughput
- <10ms median latency
- Support for Python, Ruby, Go, JavaScript, Java workers
- Feature parity with Sidekiq OSS
- Production-ready Web UI integrated into LavinMQ
- Comprehensive test coverage (>90%)
