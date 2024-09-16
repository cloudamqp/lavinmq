module RoughTime
  @@utc = Time.utc
  @@unix_ms : Int64 = @@utc.to_unix_ms // 100 * 100
  @@monotonic = Time.monotonic

  spawn(name: "RoughTime") do
    loop do
      sleep 0.1.seconds
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
end
