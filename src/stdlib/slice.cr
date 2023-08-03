require "json"
require "base64"

struct Slice(T)
  def to_json(builder : JSON::Builder)
    builder.string Base64.encode(self)
  end

  def self.from_json(json : JSON::PullParser) : Bytes
    Base64.decode_string json.read_string
  end

  # Truncate to 72 first items
  def to_s(io : IO) : Nil
    if T == UInt8
      io << "Bytes["
      first(72).join io, ", ", &.to_s(io)
      io << ", ..." if size > 72
      io << ']'
    else
      io << "Slice["
      first(72).join io, ", ", &.inspect(io)
      io << ", ..." if size > 72
      io << ']'
    end
  end
end
