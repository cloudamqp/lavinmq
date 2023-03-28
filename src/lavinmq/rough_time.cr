module RoughTime
  {% if flag?(:realtime) %}
    macro utc
      Time.utc
    end

    macro unix_ms
      Time.utc.to_unix_ms
    end

    macro monotonic
      Time.monotonic
    end
  {% else %}
    @@utc = Time.utc
    @@unix_ms : Int64 = @@utc.to_unix_ms // 100 * 100
    @@monotonic = Time.monotonic

    spawn(name: "RoughTime") do
      loop do
        sleep 0.1
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
  {% end %}
end
