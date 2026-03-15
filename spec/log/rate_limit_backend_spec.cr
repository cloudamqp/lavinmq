require "log"
require "spec"
require "../../src/lavinmq/log/rate_limit_backend"

# A minimal backend that records every entry written to it.
private class CapturingBackend < Log::Backend
  getter entries = Array(Log::Entry).new

  def initialize
    super(:direct)
  end

  def write(entry : Log::Entry) : Nil
    @entries << entry
  end
end

private def make_entry(source : String, message : String, severity = Log::Severity::Info) : Log::Entry
  Log::Entry.new(source, severity, message, Log::Metadata.empty, nil)
end

describe Log::RateLimitBackend do
  it "passes the first occurrence through immediately" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner)

    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"guest\""))

    inner.entries.size.should eq 1
    inner.entries.first.message.should eq "Authentication failure for user \"guest\""
  end

  it "suppresses repeated occurrences within the window" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)

    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"guest\""))
    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"guest\""))
    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"guest\""))

    inner.entries.size.should eq 1
  end

  it "passes through entries with different messages independently" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner)

    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"alice\""))
    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"bob\""))

    inner.entries.size.should eq 2
  end

  it "passes through entries from different sources independently" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner)

    backend.write(make_entry("amqp.connection_factory", "Authentication failure for user \"guest\""))
    backend.write(make_entry("mqtt.connection_factory", "Authentication failure for user \"guest\""))

    inner.entries.size.should eq 2
  end

  it "emits a summary then the new entry when the window expires" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 1.millisecond)

    backend.write(make_entry("src", "repeated"))
    # Suppress twice
    backend.write(make_entry("src", "repeated"))
    backend.write(make_entry("src", "repeated"))

    sleep 2.milliseconds

    # This call should trigger a summary + pass-through
    backend.write(make_entry("src", "repeated"))

    # 1 original + 1 summary + 1 new = 3
    inner.entries.size.should eq 3
    inner.entries[1].message.should contain("suppressed 2")
    inner.entries[1].message.should contain("repeated")
    inner.entries[2].message.should eq "repeated"
  end

  it "does not emit a summary when there were no suppressions" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 1.millisecond)

    backend.write(make_entry("src", "msg"))
    sleep 2.milliseconds
    backend.write(make_entry("src", "msg"))

    inner.entries.size.should eq 2
    inner.entries.none?(&.message.includes?("suppressed")).should be_true
  end

  it "cleanup does not remove fresh keys" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)

    backend.write(make_entry("src", "msg"))
    # Suppress once
    backend.write(make_entry("src", "msg"))

    backend.cleanup

    # Key is still tracked so the next write remains suppressed
    backend.write(make_entry("src", "msg"))
    inner.entries.size.should eq 1
  end

  it "close flushes pending suppressed counts as summaries" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)

    backend.write(make_entry("src", "msg"))
    backend.write(make_entry("src", "msg")) # suppress
    backend.write(make_entry("src", "msg")) # suppress

    backend.close

    # Original + summary on close
    inner.entries.size.should eq 2
    inner.entries.last.message.should contain("suppressed 2")
    inner.entries.last.message.should contain("msg")
  end

  it "close emits no summary when there are no pending suppressions" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)

    backend.write(make_entry("src", "msg"))
    backend.close

    inner.entries.size.should eq 1
  end

  it "passes through entries from non-configured sources without rate limiting" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, sources: ["amqp.connection_factory"])

    # This source is not in the list — all writes pass through
    3.times { backend.write(make_entry("other.source", "msg")) }
    inner.entries.size.should eq 3

    # This source IS in the list — second write is suppressed
    backend.write(make_entry("amqp.connection_factory", "Auth failure"))
    backend.write(make_entry("amqp.connection_factory", "Auth failure"))
    inner.entries.size.should eq 4
  end

  it "does not grow beyond MAX_ENTRIES" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner)

    Log::RateLimitBackend::MAX_ENTRIES.times do |i|
      backend.write(make_entry("src", "unique message #{i}"))
    end

    # One beyond the cap — still passes through (fail-open), but is not stored
    backend.write(make_entry("src", "overflow"))
    backend.write(make_entry("src", "overflow"))

    # Both overflow writes pass through since the key was never stored
    overflow_count = inner.entries.count { |e| e.message == "overflow" }
    overflow_count.should eq 2
  end

  it "preserves source and severity in summary entries" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 1.millisecond)

    entry = make_entry("amqp.connection_factory", "Auth failure", Log::Severity::Warn)
    backend.write(entry)
    backend.write(entry) # suppress

    sleep 2.milliseconds
    backend.write(entry) # triggers summary

    summary = inner.entries[1]
    summary.source.should eq "amqp.connection_factory"
    summary.severity.should eq Log::Severity::Warn
  end

  it "write after close is a no-op" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)

    backend.write(make_entry("src", "msg"))
    backend.close
    backend.write(make_entry("src", "msg")) # must not raise or write

    inner.entries.size.should eq 1
  end

  it "close is idempotent" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)
    backend.write(make_entry("src", "msg"))
    backend.close
    backend.close # must not raise
  end

  it "cleanup flushes suppressed counts for stale entries" do
    inner = CapturingBackend.new
    backend = Log::RateLimitBackend.new(inner, 5.seconds)

    backend.write(make_entry("src", "msg"))
    backend.write(make_entry("src", "msg")) # suppress
    backend.write(make_entry("src", "msg")) # suppress

    # Age the entry so cleanup considers it stale
    backend.@states.each_value do |s|
      s.last_seen = Time.instant - Log::RateLimitBackend::STALE_THRESHOLD - 1.second
    end

    backend.cleanup

    inner.entries.size.should eq 2
    inner.entries.last.message.should contain("suppressed 2")
  end
end
