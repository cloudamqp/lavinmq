require "json"

class Regex
  def self.new(pull : JSON::PullParser)
    pattern = pull.read_string
    new(pattern)
  end

  def to_json(io)
    source.to_json(io)
  end

  def self.from_json(pull)
    new(pull.read_string)
  end
end
