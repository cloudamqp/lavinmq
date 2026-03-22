require "log"

# A `Log::Backend` wrapper that suppresses repeated log entries within a
# configurable window. The first occurrence of each {source, message} pair
# passes through immediately. Subsequent occurrences within the suppression
# window are counted but not forwarded. When the window expires and a new
# entry arrives, a summary entry ("… suppressed N times: <message>") is
# written first, followed by the new entry.
#
# Only entries from sources in `sources` are rate-limited; all other entries
# pass through immediately with no overhead. Pass an empty array to rate-limit
# all sources (not recommended in production).
#
# Pending suppressed counts are flushed as summaries on `close` and during
# periodic cleanup of stale keys. Call `close` when the backend is no longer
# needed to stop the cleanup fiber and flush any pending summaries.
class Log::RateLimitBackend < Log::Backend
  SUPPRESSION_WINDOW = 5.seconds
  CLEANUP_INTERVAL   = 30.seconds
  STALE_THRESHOLD    = 60.seconds
  MAX_ENTRIES        = 10_000

  private record Key, source : String, message : String

  private class State
    property suppressed : Int32 = 0
    property window_start : Time::Instant
    property last_seen : Time::Instant
    property severity : Log::Severity

    def initialize(@window_start : Time::Instant, @last_seen : Time::Instant,
                   @severity : Log::Severity = Log::Severity::Info)
    end
  end

  def initialize(@backend : Log::Backend,
                 @window : Time::Span = SUPPRESSION_WINDOW,
                 @sources : Array(String) = [] of String)
    super(:direct)
    @states = Hash(Key, State).new
    @lock = Mutex.new
    @stop = Channel(Nil).new
    @closed = false
    spawn_cleanup_loop
  end

  def write(entry : Log::Entry) : Nil
    return if @closed
    unless @sources.empty? || @sources.includes?(entry.source)
      @backend.write(entry)
      return
    end

    key = Key.new(entry.source, entry.message)
    now = Time.instant
    summary, pass_through = @lock.synchronize { update_state(key, now, entry.severity) }

    if summary_count = summary
      @backend.write(build_summary(entry.source, entry.severity, summary_count, entry.message, entry.timestamp))
    end

    @backend.write(entry) if pass_through
  end

  def close : Nil
    pending = @lock.synchronize do
      return if @closed
      @closed = true
      entries = @states.select { |_, state| state.suppressed > 0 }.to_a
      @states.clear
      entries
    end
    flush_summaries(pending)
    @stop.close
    @backend.close
  end

  # Returns {suppressed_count_or_nil, pass_through}.
  # suppressed_count_or_nil is non-nil when a summary should be emitted first.
  private def update_state(key : Key, now : Time::Instant, severity : Log::Severity) : {Int32?, Bool}
    if state = @states[key]?
      state.last_seen = now
      state.severity = severity
      elapsed = now - state.window_start
      if elapsed >= @window
        suppressed = state.suppressed
        state.suppressed = 0
        state.window_start = now
        summary = suppressed > 0 ? suppressed : nil
        {summary, true}
      else
        state.suppressed += 1
        {nil, false}
      end
    else
      if @states.size < MAX_ENTRIES
        @states[key] = State.new(window_start: now, last_seen: now, severity: severity)
      end
      {nil, true}
    end
  end

  def cleanup : Nil
    to_flush = Array({Key, State}).new
    now = Time.instant
    @lock.synchronize do
      return if @closed
      @states.reject! do |key, state|
        if now - state.last_seen > STALE_THRESHOLD
          to_flush << {key, state} if state.suppressed > 0
          true
        else
          false
        end
      end
    end
    flush_summaries(to_flush)
  end

  private def flush_summaries(entries : Array({Key, State})) : Nil
    now = Time.utc
    entries.each do |key, state|
      @backend.write(build_summary(key.source, state.severity, state.suppressed, key.message, now))
    end
  end

  private def build_summary(source : String, severity : Log::Severity,
                            count : Int32, original : String, timestamp : Time) : Log::Entry
    Log::Entry.new(
      source,
      severity,
      "… suppressed #{count} time#{count == 1 ? "" : "s"}: #{original}",
      Log::Metadata.empty,
      nil,
      timestamp: timestamp
    )
  end

  private def spawn_cleanup_loop : Nil
    spawn(name: "rate-limit-backend-cleanup") do
      loop do
        select
        when @stop.receive?
          break
        when timeout(CLEANUP_INTERVAL)
          cleanup
        end
      rescue ex
        ::Log.warn(exception: ex) { "rate-limit-backend cleanup failed" }
      end
    end
  end
end
