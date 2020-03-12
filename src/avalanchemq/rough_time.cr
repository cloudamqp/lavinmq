class RoughTime
  @@t = Time.utc

  spawn(name: "RoughTime") do
    loop do
      sleep 1
      @@t = Time.utc
    end
  end

  def self.utc
    @@t
  end
end
