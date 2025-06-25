module RoughTime
  @@paused_utc = Time.utc
  @@paused_unix_ms : Int64 = @@paused_utc.to_unix_ms // 100 * 100
  @@paused_monotonic = Time.monotonic
  @@paused = false

  def self.utc : Time
    if @@paused
      return @@paused_utc
    end
    previous_def
  end

  def self.unix_ms : Int64
    if @@paused
      return @@paused_unix_ms
    end
    previous_def
  end

  def self.monotonic : Time::Span
    if @@paused
      return @@paused_monotonic
    end
    previous_def
  end

  def self.pause
    @@paused = true
    @@paused_utc = Time.utc
    @@paused_unix_ms = @@paused_utc.to_unix_ms // 100 * 100
    @@paused_monotonic = Time.monotonic
  end

  def self.travel(time : Time::Span)
    @@paused_utc += time
    @@paused_unix_ms += @@paused_utc.to_unix_ms // 100 * 100
    @@paused_monotonic += time
  end

  def self.resume
    @@paused = false
  end

  def self.paused(&)
    self.pause
    yield self
  ensure
    self.resume
  end
end
