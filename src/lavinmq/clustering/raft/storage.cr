require "digest/crc32"
require "./core"
require "../../filesystem"

module LavinMQ::Clustering::Raft
  # Persists a HardState as JSON with a CRC32 trailer. The state is a
  # handful of bytes (ISR entries are compacted on commit), so every save
  # rewrites the whole file: a temp file, fsynced and renamed over the old
  # one, whatever the `sync` setting, since a vote mustn't be lost.
  class Storage
    Format = IO::ByteFormat::LittleEndian

    class CorruptError < Exception; end

    getter path : String

    def initialize(dir : String)
      @dir = dir
      @path = File.join(dir, ".raft_state")
    end

    def load : HardState?
      bytes = File.open(@path, &.getb_to_end)
      raise CorruptError.new("#{@path} is truncated") if bytes.size < 4
      body = bytes[0, bytes.size - 4]
      stored_crc = Format.decode(UInt32, bytes[bytes.size - 4, 4])
      raise CorruptError.new("#{@path} checksum mismatch") unless Digest::CRC32.checksum(body) == stored_crc
      HardState.from_json(String.new(body))
    rescue File::NotFoundError
      nil
    rescue ex : JSON::ParseException | JSON::SerializableError
      raise CorruptError.new("#{@path} is invalid: #{ex.message}")
    end

    def save(state : HardState) : Nil
      body = state.to_json
      FileSystem.replace(@path, sync: true) do |f|
        f.write body.to_slice
        f.write_bytes Digest::CRC32.checksum(body), Format
      end
    end
  end
end
