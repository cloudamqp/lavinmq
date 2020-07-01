abstract class TimeObject
  @@t = Time.utc
  def self.utc : Time
    @@t
  end
end

class RoughTime < TimeObject
  @@t = Time.utc
  spawn(name: "RoughTime") do
    loop do
      sleep 1
      @@t = Time.utc
    end
  end
end

class MockRoughTime < TimeObject
  @@t = Time.utc
  def self.update_time(time : Time)
    @@t = time
  end

  def self.next_second
    update_time(@@t + 1.seconds)
  end

  def self.restore
    @@t = Time.utc
  end
end
