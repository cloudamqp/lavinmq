require "./logger"

module LavinMQ
  # Tracks authentication failures per {username, ip} pair
  # to suppress repeated log messages. Thread-safe for Crystal
  # fibers (single-threaded cooperative scheduling).
  class AuthFailureTracker
    SUPPRESSION_WINDOW = 5.seconds
    CLEANUP_INTERVAL   = 30.seconds
    STALE_THRESHOLD    = 60.seconds

    private record Key, username : String, ip : String

    private struct Entry
      property count : Int32
      property last_logged_at : Time::Instant
      property last_seen_at : Time::Instant

      def initialize(@last_logged_at, @last_seen_at, @count = 0)
      end
    end

    def initialize
      @failures = Hash(Key, Entry).new
      spawn_cleanup_loop
    end

    # Returns true if the failure should be logged now.
    # When returning true after suppression, yields the
    # number of suppressed messages so callers can include
    # a summary.
    def track(username : String, ip : String, & : Int32 ->) : Bool
      key = Key.new(username, ip)
      now = Time.instant

      if entry = @failures[key]?
        entry.last_seen_at = now
        elapsed = now - entry.last_logged_at
        if elapsed >= SUPPRESSION_WINDOW
          suppressed = entry.count
          entry.count = 0
          entry.last_logged_at = now
          @failures[key] = entry
          yield suppressed
          true
        else
          entry.count += 1
          @failures[key] = entry
          false
        end
      else
        @failures[key] = Entry.new(
          last_logged_at: now,
          last_seen_at: now,
        )
        yield 0
        true
      end
    end

    private def spawn_cleanup_loop
      spawn(name: "auth-failure-tracker-cleanup") do
        loop do
          sleep CLEANUP_INTERVAL
          cleanup
        end
      end
    end

    def cleanup
      now = Time.instant
      @failures.reject! do |_key, entry|
        now - entry.last_seen_at > STALE_THRESHOLD
      end
    end
  end
end
