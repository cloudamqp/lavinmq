require "amq-protocol"
require "../../mfile"
require "../../clustering/replicator"

module LavinMQ::AMQP
  # Owns a stream's `consumer_offsets` file: an append-only log of
  # {consumer_tag, offset} entries used to resume consumers where they left
  # off. The file is compacted (via `cleanup`) to drop offsets that have fallen
  # out of the stream and to keep the file size bounded.
  class ConsumerOffsets
    Log = LavinMQ::Log.for "consumer_offsets"

    ENTRY_OVERHEAD = 1 + 8                # ShortString length byte + Int64 offset
    MAX_ENTRY_SIZE = 255 + ENTRY_OVERHEAD # Largest possible entry: 255-byte tag + Int64

    # Size bounds for the compacted consumer_offsets file.
    MAX_FILE_SIZE = 64_i64 * 1024 * 1024 # 64 MiB
    MIN_FILE_SIZE = 8_i64 * 1024         # 8 KiB

    @mfile : MFile
    @positions = Hash(String, Int64).new # consumer_tag => file position of its offset

    def initialize(dir : String, capacity : Int, @replicator : Clustering::Replicator?)
      @mfile = MFile.new(File.join(dir, "consumer_offsets"), capacity)
      @replicator.try &.register_file @mfile
      restore_positions
    end

    # Current size of the offsets file on disk.
    def size
      @mfile.size
    end

    def close : Nil
      @mfile.close
    end

    def delete : Nil
      @mfile.delete(raise_on_missing: false)
      @replicator.try &.delete_file(@mfile.path)
      @mfile.close
    end

    def last_offset_by_tag(consumer_tag)
      if pos = @positions[consumer_tag]?
        tx = @mfile.to_slice(pos, 8)
        return IO::ByteFormat::LittleEndian.decode(Int64, tx)
      end
    end

    # Appends `new_offset` for `consumer_tag`. Compacts the file first if the
    # append wouldn't fit; the block yields the lowest offset still in the
    # stream so stale offsets can be dropped during compaction.
    def store(consumer_tag : String, new_offset : Int64, & : -> Int64)
      cleanup { yield } if full?(consumer_tag)
      write(consumer_tag, new_offset)
    end

    # Compacts the file, dropping offsets that are no longer in the stream and
    # capping the file size. The block yields the lowest offset still in the
    # stream; it is only evaluated when there is something to compact.
    def cleanup(& : -> Int64)
      return if @mfile.size.zero?

      lowest_offset_in_stream = yield

      # Offsets still within the stream (higher position == more recently committed).
      tracked_offsets = Array(Tuple(String, Int64, Int64)).new
      @positions.each do |ctag, pos|
        if (offset = last_offset_by_tag(ctag)) && offset >= lowest_offset_in_stream
          tracked_offsets << {ctag, offset, pos}
        end
      end
      @positions = Hash(String, Int64).new

      # Reserve room for one max-length entry so the append that triggered
      # cleanup always fits in the rewritten file.
      budget = MAX_FILE_SIZE - MAX_ENTRY_SIZE
      offsets_to_save = ConsumerOffsets.trim_to_size(tracked_offsets, budget)
      if (dropped = tracked_offsets.size - offsets_to_save.size) > 0
        Log.warn { "Consumer offsets file #{@mfile.path} is full, dropped #{dropped} oldest consumer offset(s)" }
      end
      used_bytes = offsets_to_save.sum { |ctag, _offset, _pos| ConsumerOffsets.entry_size(ctag) }
      # Allocate 1000x the used size as headroom for future appends, bounded by the min/max file size.
      new_capacity = (used_bytes * 1000).clamp(MIN_FILE_SIZE, MAX_FILE_SIZE)
      replace_file(new_capacity) do
        offsets_to_save.each do |ctag, offset, _pos|
          write(ctag, offset)
        end
      end
    end

    private def write(consumer_tag : String, new_offset : Int64)
      start_pos = @mfile.size
      @mfile.write_bytes AMQ::Protocol::ShortString.new(consumer_tag)
      @positions[consumer_tag] = @mfile.size
      @mfile.write_bytes new_offset
      @replicator.try &.append(@mfile.path, start_pos, ConsumerOffsets.entry_size(consumer_tag))
    end

    private def full?(consumer_tag)
      (@mfile.size + ConsumerOffsets.entry_size(consumer_tag)) >= @mfile.capacity
    end

    private def replace_file(capacity : Int, &)
      @replicator.try &.delete_file(@mfile.path) # FIXME: this is not entirely safe, but replace_file is worse
      old_mfile = @mfile
      @mfile = MFile.new("#{old_mfile.path}.tmp", capacity)
      yield # fill the new file with correct data in this block
      @mfile.rename(old_mfile.path)
      old_mfile.close(truncate_to_size: false)
    end

    private def restore_positions : Nil
      return if @mfile.size.zero?

      loop do
        ctag = AMQ::Protocol::ShortString.from_io(@mfile)
        break if ctag.empty?
        @positions[ctag] = @mfile.pos
        @mfile.skip(8)
      rescue IO::EOFError
        break
      end
      @mfile.pos = 0 if @mfile.pos == 1
      @mfile.resize(@mfile.pos)
    end

    # Encoded size of one {consumer_tag, offset} entry in the offsets file.
    def self.entry_size(consumer_tag : String) : Int64
      (consumer_tag.bytesize + ENTRY_OVERHEAD).to_i64
    end

    # Drops the oldest (lowest position) offsets until the set fits in
    # `max_size`. Survivors are returned oldest first so rewriting them keeps
    # file position reflecting commit recency. Usually drops nothing.
    def self.trim_to_size(offsets : Array(Tuple(String, Int64, Int64)),
                          max_size : Int64) : Array(Tuple(String, Int64, Int64))
      offsets.sort_by! { |_ctag, _offset, pos| pos } # oldest (lowest position) first

      total_size = offsets.sum { |ctag, _offset, _pos| entry_size(ctag) }
      drop_count = 0
      # `>=` so the kept set stays strictly below the cap, leaving headroom.
      while total_size >= max_size
        total_size -= entry_size(offsets[drop_count][0])
        drop_count += 1
      end
      offsets[drop_count..]
    end
  end
end
