require "json"
require "base64"

struct Slice(T)
  def to_json(builder : JSON::Builder)
    builder.string Base64.encode(self)
  end

  def self.from_json(json : JSON::PullParser) : Bytes
    Base64.decode_string json.read_string
  end
end
