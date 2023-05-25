module RoughTime
  @@utc = Time.utc
  @@unix_ms : Int64 = @@utc.to_unix_ms // 100 * 100
  @@monotonic = Time.monotonic
  @@roughtime_timer = 0.1

  spawn(name: "RoughTime") do
    loop do
      sleep @@roughtime_timer
      @@utc = Time.utc
      @@unix_ms = @@utc.to_unix_ms // 100 * 100
      @@monotonic = Time.monotonic
    end
  end

  def self.utc : Time
    @@utc
  end

  def self.unix_ms : Int64
    @@unix_ms
  end

  def self.monotonic : Time::Span
    @@monotonic
  end

  def self.update_timer(roughtime_timer : Float64)
    @@roughtime_timer = roughtime_timer
  end
end
