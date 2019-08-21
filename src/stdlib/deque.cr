require "json"

class Deque(T)
  def to_json(builder : JSON::Builder)
    builder.array do
      each do |v|
        v.to_json(builder)
      end
    end
  end

  def self.from_json(json : JSON::PullParser) : Deque(T)
    q = Deque(T).new
    json.read_array do
      q << T.from_json(json)
    end
    q
  end
end
