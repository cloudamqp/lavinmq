require "digest/crc32"
require "./core"

module LavinMQ::Clustering::Raft
  # Persists a HardState. The state is a handful of bytes (ISR entries are
  # compacted on commit), so every save rewrites the whole file: write a temp
  # file, fsync it, rename over the old one and fsync the directory.
  class Storage
    MAGIC   = "LMQRAFT"
    VERSION = 2u8
    Format  = IO::ByteFormat::LittleEndian
    # Uncommitted entries are few, this only guards against a corrupt count.
    MAX_ENTRIES = 1 << 20

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
      stored_crc = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[bytes.size - 4, 4])
      raise CorruptError.new("#{@path} checksum mismatch") unless Digest::CRC32.checksum(body) == stored_crc
      decode(IO::Memory.new(body, writable: false))
    rescue File::NotFoundError
      nil
    rescue IO::EOFError
      raise CorruptError.new("#{@path} is truncated")
    end

    def save(state : HardState) : Nil
      io = IO::Memory.new
      encode(io, state)
      io.write_bytes Digest::CRC32.checksum(io.to_slice), Format
      tmp = "#{@path}.tmp"
      File.open(tmp, "w") do |f|
        f.write io.to_slice
        f.fsync
      end
      File.rename(tmp, @path)
      File.open(@dir, &.fsync)
    end

    private def encode(io, state : HardState) : Nil
      io.write MAGIC.to_slice
      io.write_byte VERSION
      io.write_bytes state.term, Format
      voted_for = state.voted_for || ""
      io.write_bytes voted_for.bytesize, Format
      io.write voted_for.to_slice
      io.write_bytes state.snapshot_index, Format
      io.write_bytes state.snapshot_term, Format
      Codec.write_isr(io, state.snapshot_isr)
      Codec.write_membership(io, state.snapshot_membership)
      Codec.write_entries(io, state.entries)
      io.write_bytes state.peer_node_ids.size, Format
      state.peer_node_ids.each do |addr, node_id|
        io.write_bytes addr.bytesize, Format
        io.write addr.to_slice
        io.write_bytes node_id, Format
      end
    end

    private def decode(io) : HardState
      raise CorruptError.new("#{@path} has an invalid header") unless io.read_string(MAGIC.bytesize) == MAGIC
      version = io.read_byte
      raise CorruptError.new("#{@path} has unsupported version #{version}") unless version == VERSION
      term = io.read_bytes Int64, Format
      voted_for = io.read_string(io.read_bytes(Int32, Format))
      snapshot_index = io.read_bytes Int64, Format
      snapshot_term = io.read_bytes Int64, Format
      snapshot_isr = Codec.read_isr(io)
      snapshot_membership = Codec.read_membership(io)
      entries = Codec.read_entries(io, MAX_ENTRIES)
      peer_node_ids = Hash(String, Int32).new
      io.read_bytes(Int32, Format).times do
        addr = io.read_string(io.read_bytes(Int32, Format))
        peer_node_ids[addr] = io.read_bytes(Int32, Format)
      end
      HardState.new(term, voted_for.presence, snapshot_index, snapshot_term, snapshot_isr, entries, peer_node_ids,
        snapshot_membership)
    end
  end
end
