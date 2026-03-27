#!/usr/bin/env ruby
# frozen_string_literal: true

# Test script for LavinMQ queue retry feature
# Uses amqp-client gem: gem install amqp-client

require "amqp-client"
require "timeout"

AMQP_URL = ENV.fetch("AMQP_URL", "amqp://guest:guest@localhost")

def section(title)
  puts
  puts "=" * 60
  puts "  #{title}"
  puts "=" * 60
end

def assert(label, condition)
  if condition
    puts "  PASS: #{label}"
  else
    puts "  FAIL: #{label}"
    exit 1
  end
end

def connect
  client = AMQP::Client.new(AMQP_URL)
  client.connect
end

def poll_for(ch, queue, no_ack: false, timeout: 5)
  deadline = Time.now + timeout
  loop do
    msg = ch.basic_get(queue, no_ack: no_ack)
    return msg if msg
    return nil if Time.now > deadline
    sleep 0.1
  end
end

# --------------------------------------------------------------------------
# Case 1: Basic retry on reject (requeue=false) sets x-retry-count header
# --------------------------------------------------------------------------
section "Case 1: Basic retry on reject"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-basic") rescue nil
  ch.queue_declare("test-retry-basic", arguments: {
    "x-retry-max-count" => 3,
    "x-retry-delay"     => 500,
  })
  ch.basic_publish_confirm("hello retry", exchange: "", routing_key: "test-retry-basic")

  msg = ch.basic_get("test-retry-basic", no_ack: false)
  assert "got original message", msg
  assert "body is correct", msg.body == "hello retry"
  msg.reject

  puts "  waiting for retry delay (500ms + margin)..."
  msg2 = poll_for(ch, "test-retry-basic", no_ack: true)
  assert "message redelivered after retry", msg2
  assert "body preserved", msg2.body == "hello retry"

  headers = msg2.properties.headers
  assert "x-retry-count header present", headers && headers["x-retry-count"]
  assert "x-retry-count is 1", headers["x-retry-count"] == 1
  puts "  OK"
ensure
  ch.queue_delete("test-retry-basic") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 2: Incrementing retry count across multiple rejections
# --------------------------------------------------------------------------
section "Case 2: Incrementing retry count"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-incr") rescue nil
  ch.queue_declare("test-retry-incr", arguments: {
    "x-retry-max-count" => 5,
    "x-retry-delay"     => 300,
  })
  ch.basic_publish_confirm("count me", exchange: "", routing_key: "test-retry-incr")

  3.times do |i|
    msg = poll_for(ch, "test-retry-incr")
    assert "got message on iteration #{i}", msg
    if i > 0
      headers = msg.properties.headers
      assert "retry count is #{i} on iteration #{i}", headers && headers["x-retry-count"] == i
    end
    msg.reject
    puts "  rejected message (iteration #{i}), waiting for retry..."
  end
  puts "  OK"
ensure
  ch.queue_delete("test-retry-incr") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 3: Dead-lettering after max retries exhausted (with DLX)
# --------------------------------------------------------------------------
section "Case 3: Dead-letter after max retries with DLX"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-dlx") rescue nil
  ch.queue_delete("test-retry-dlq") rescue nil
  ch.queue_declare("test-retry-dlq")
  ch.queue_declare("test-retry-dlx", arguments: {
    "x-retry-max-count"         => 1,
    "x-retry-delay"             => 300,
    "x-dead-letter-exchange"    => "",
    "x-dead-letter-routing-key" => "test-retry-dlq",
  })
  ch.basic_publish_confirm("dlx me", exchange: "", routing_key: "test-retry-dlx")

  msg = poll_for(ch, "test-retry-dlx")
  assert "got original", msg
  msg.reject
  puts "  rejected once, waiting for retry..."

  msg = poll_for(ch, "test-retry-dlx")
  assert "got retried message", msg
  assert "x-retry-count is 1", msg.properties.headers && msg.properties.headers["x-retry-count"] == 1
  msg.reject
  puts "  rejected again (max exhausted), should go to DLQ..."
  sleep 0.5

  dlq_msg = poll_for(ch, "test-retry-dlq", no_ack: true)
  assert "message landed in DLQ", dlq_msg
  assert "DLQ body correct", dlq_msg.body == "dlx me"
  puts "  OK"
ensure
  ch.queue_delete("test-retry-dlx") rescue nil
  ch.queue_delete("test-retry-dlq") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 4: Discard after max retries (no DLX configured)
# --------------------------------------------------------------------------
section "Case 4: Discard after max retries (no DLX)"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-discard") rescue nil
  ch.queue_declare("test-retry-discard", arguments: {
    "x-retry-max-count" => 1,
    "x-retry-delay"     => 300,
  })
  ch.basic_publish_confirm("discard me", exchange: "", routing_key: "test-retry-discard")

  msg = poll_for(ch, "test-retry-discard")
  assert "got original", msg
  msg.reject

  msg = poll_for(ch, "test-retry-discard")
  assert "got retried message", msg
  msg.reject
  sleep 0.5

  msg = ch.basic_get("test-retry-discard", no_ack: true)
  assert "message discarded (queue empty)", msg.nil?
  puts "  OK"
ensure
  ch.queue_delete("test-retry-discard") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 5: requeue=true should NOT trigger retry
# --------------------------------------------------------------------------
section "Case 5: requeue=true does not trigger retry"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-requeue") rescue nil
  ch.queue_declare("test-retry-requeue", arguments: {
    "x-retry-max-count" => 3,
    "x-retry-delay"     => 300,
  })
  ch.basic_publish_confirm("requeue me", exchange: "", routing_key: "test-retry-requeue")

  msg = poll_for(ch, "test-retry-requeue")
  assert "got message", msg
  msg.reject(requeue: true)
  sleep 0.3

  msg2 = ch.basic_get("test-retry-requeue", no_ack: true)
  assert "message requeued normally", msg2
  assert "body correct", msg2.body == "requeue me"
  headers = msg2.properties.headers
  has_retry = headers && headers.key?("x-retry-count")
  assert "no x-retry-count header (normal requeue)", !has_retry
  puts "  OK"
ensure
  ch.queue_delete("test-retry-requeue") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 6: Internal retry queues are not accessible
# --------------------------------------------------------------------------
section "Case 6: Internal retry queues not accessible"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-internal") rescue nil
  ch.queue_declare("test-retry-internal", arguments: {
    "x-retry-max-count" => 3,
    "x-retry-delay"     => 500,
  })

  begin
    ch2 = conn.channel
    ch2.queue_declare("test-retry-internal.x-retry.0", passive: true)
    assert "should not be able to access internal retry queue", false
  rescue => e
    assert "access refused on internal retry queue", e.message.to_s =~ /ACCESS.REFUSED/i
    puts "  (got expected error: #{e.message})"
  end
  puts "  OK"
ensure
  ch.queue_delete("test-retry-internal") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 7: Consumer-based retry (subscribe instead of polling)
# --------------------------------------------------------------------------
section "Case 7: Consumer-based retry with subscribe"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-consumer") rescue nil
  ch.queue_declare("test-retry-consumer", arguments: {
    "x-retry-max-count" => 2,
    "x-retry-delay"     => 300,
  })

  received = []
  done = Queue.new

  ch.basic_consume("test-retry-consumer", no_ack: false) do |msg|
    headers = msg.properties.headers
    retry_count = headers && headers["x-retry-count"] ? headers["x-retry-count"].to_i : 0
    received << { body: msg.body, retry_count: retry_count }
    puts "  consumer got: body=#{msg.body.inspect}, retry_count=#{retry_count}"

    if retry_count < 2
      msg.reject
    else
      msg.ack
      done << :done
    end
  end

  ch.basic_publish_confirm("consumer test", exchange: "", routing_key: "test-retry-consumer")

  Timeout.timeout(15) { done.pop }

  assert "received 3 deliveries (original + 2 retries)", received.size == 3
  assert "first delivery has retry_count 0", received[0][:retry_count] == 0
  assert "second delivery has retry_count 1", received[1][:retry_count] == 1
  assert "third delivery has retry_count 2", received[2][:retry_count] == 2
  puts "  OK"
ensure
  ch.queue_delete("test-retry-consumer") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 8: Multiple messages retrying concurrently
# --------------------------------------------------------------------------
section "Case 8: Multiple messages retrying concurrently"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-multi") rescue nil
  ch.queue_declare("test-retry-multi", arguments: {
    "x-retry-max-count" => 2,
    "x-retry-delay"     => 300,
  })

  5.times do |i|
    ch.basic_publish_confirm("msg-#{i}", exchange: "", routing_key: "test-retry-multi")
  end

  5.times do
    msg = poll_for(ch, "test-retry-multi")
    assert "got a message to reject", msg
    msg.reject
  end

  puts "  rejected 5 messages, waiting for retries..."

  bodies = []
  deadline = Time.now + 5
  while bodies.size < 5 && Time.now < deadline
    msg = ch.basic_get("test-retry-multi", no_ack: true)
    if msg
      bodies << msg.body
      headers = msg.properties.headers
      assert "retry count is 1 for #{msg.body}", headers && headers["x-retry-count"] == 1
    else
      sleep 0.1
    end
  end
  assert "all 5 messages retried", bodies.size == 5
  puts "  OK"
ensure
  ch.queue_delete("test-retry-multi") rescue nil
  conn.close
end

# --------------------------------------------------------------------------
# Case 9: x-retry-delay-multiplier increases delay between retries
# --------------------------------------------------------------------------
section "Case 9: x-retry-delay-multiplier"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-multiplier") rescue nil
  # delay=200, multiplier=3 => retry 0: 200ms, retry 1: 600ms, retry 2: 1800ms
  ch.queue_declare("test-retry-multiplier", arguments: {
    "x-retry-max-count"        => 3,
    "x-retry-delay"            => 200,
    "x-retry-delay-multiplier" => 3,
  })
  ch.basic_publish_confirm("multiplier test", exchange: "", routing_key: "test-retry-multiplier")

  msg = poll_for(ch, "test-retry-multiplier")
  assert "got original", msg
  msg.reject
  t0 = Time.now
  puts "  rejected original, waiting for retry 1 (expect ~200ms)..."

  msg = poll_for(ch, "test-retry-multiplier", timeout: 5)
  elapsed1 = Time.now - t0
  assert "got retry 1", msg
  assert "x-retry-count is 1", msg.properties.headers && msg.properties.headers["x-retry-count"] == 1
  puts "  retry 1 arrived after #{(elapsed1 * 1000).to_i}ms (expected ~200ms)"

  msg.reject
  t1 = Time.now
  puts "  rejected retry 1, waiting for retry 2 (expect ~600ms)..."

  msg = poll_for(ch, "test-retry-multiplier", timeout: 5)
  elapsed2 = Time.now - t1
  assert "got retry 2", msg
  assert "x-retry-count is 2", msg.properties.headers && msg.properties.headers["x-retry-count"] == 2
  puts "  retry 2 arrived after #{(elapsed2 * 1000).to_i}ms (expected ~600ms)"
  assert "retry 2 took longer than retry 1", elapsed2 > elapsed1

  msg.ack
  puts "  OK"
ensure
  ch.queue_delete("test-retry-multiplier") rescue nil
  conn.close
end

sleep 10
# --------------------------------------------------------------------------
# Case 10: 30 retries with multiplier, then discard
# --------------------------------------------------------------------------
section "Case 10: 30 retries with multiplier, then discard"

conn = connect
ch = conn.channel
begin
  ch.queue_delete("test-retry-30") rescue nil
  # delay=100ms, multiplier=2, max_delay capped at 500ms to keep total time reasonable
  # TTLs: 100, 200, 400, 500, 500, 500, ... (capped at 500 from level 3 onward)
  ch.queue_declare("test-retry-30", arguments: {
    "x-retry-max-count"        => 30,
    "x-retry-delay"            => 500,
    "x-retry-delay-multiplier" => 2,
    "x-retry-max-delay"        => 10000,
  })
  ch.basic_publish_confirm("retry 30 times", exchange: "", routing_key: "test-retry-30")

  prev_elapsed = 0.0
  # Iteration 0 = original delivery, iterations 1..30 = retries
  31.times do |i|
    t = Time.now
    msg = poll_for(ch, "test-retry-30", timeout: 15)
    elapsed = Time.now - t
    assert "got message on delivery #{i}", msg

    headers = msg.properties.headers
    if i == 0
      has_retry = headers && headers.key?("x-retry-count")
      assert "no x-retry-count on original", !has_retry
    else
      assert "x-retry-count is #{i}", headers && headers["x-retry-count"] == i
      # First few retries should show increasing delay due to multiplier
      if i >= 2 && i <= 3
        assert "retry #{i} delay (#{(elapsed * 1000).to_i}ms) >= retry #{i - 1} delay (#{(prev_elapsed * 1000).to_i}ms)",
               elapsed >= prev_elapsed * 0.8 # 0.8 factor for timing jitter
      end
    end

    printf "  delivery %2d: x-retry-count=%-2s  elapsed=%4dms\n",
           i, i == 0 ? "-" : i.to_s, (elapsed * 1000).to_i
    prev_elapsed = elapsed
    msg.reject
  end

  # After 30 retries exhausted, the 31st reject should discard (no DLX configured)
  puts "  all 30 retries exhausted, message should be discarded..."
  sleep 1.0
  msg = ch.basic_get("test-retry-30", no_ack: true)
  assert "message discarded after 30 retries (queue empty)", msg.nil?
  puts "  OK"
ensure
  ch.queue_delete("test-retry-30") rescue nil
  conn.close
end

puts
puts "=" * 60
puts "  ALL CASES PASSED"
puts "=" * 60
