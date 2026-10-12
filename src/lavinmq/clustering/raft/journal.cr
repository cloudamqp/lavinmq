require "digest/crc32"
require "./log_store"
require "../../logger"
require "../../schema"

module LavinMQ::Clustering::Raft
  # A LogStore persisted as an append-only journal, `.raft_log.<generation>`
  # in the data dir, next to `.raft_state`, which says which generation is
  # current. The entries are kept in memory too, Core reads them from there.
  #
  # Changing the log only queues records, Core stays free of IO. #sync writes
  # and fsyncs them, and must be done before anything that depends on them
  # leaves the node, see Node#save.
  #
  # A record is `[length u32][crc32 u32][type u8][body]`, little endian, the
  # length and the checksum covering the type and the body. Records are
  # appended entries and truncations. Compaction is in `.raft_state`, the
  # journal skips what it already has. Installing a snapshot, or a journal
  # that's mostly compacted entries, starts a new generation holding only the
  # live entries. `.raft_state` is saved naming it before the old one is
  # deleted (#drop_old_generations), so a crash anywhere leaves `.raft_state`
  # naming a whole journal.
  #
  # On opening, a record that is cut short or fails its checksum ends the
  # journal: it's what a crash leaves when it happens before the fsync, and
  # nothing it held was acknowledged. It's cut off, so appending can continue.
  class JournalLogStore < LogStore
    Log    = LavinMQ::Log.for "clustering.raft.journal"
    Format = IO::ByteFormat::LittleEndian
    PREFIX = Schema::RAFT_LOG_PREFIX
    HEADER = 8
    # Start a new generation when compacted records are more than half of the
    # journal, and the journal is larger than this
    ROTATE_BYTES = 1 << 20

    enum RecordType : UInt8
      Append   = 1
      Truncate = 2
      # The entries up to this index are durable, their payload too, see
      # #mark_durable
      Durable = 3
    end

    class CorruptError < Exception; end

    getter generation : Int64
    @mem : MemoryLogStore
    @dir : String
    @file : File? = nil
    # The current generation's file is to be created at the next #sync
    @creating = false
    @buffer = IO::Memory.new
    @scratch = IO::Memory.new
    # Generations replaced since the last #drop_old_generations
    @old_generations = Array(Int64).new
    # Bytes in the current generation's file, written and buffered
    @journal_bytes = 0i64
    # Size of the record that appended each live entry, and their sum
    @record_sizes = Deque(Int32).new
    # Bytes of the records of the live entries, what a new generation holds
    getter live_bytes = 0i64
    # The last index marked durable, see #mark_durable
    @durable_mark : Int64? = nil

    # Opens generation *generation* in *dir*, after the snapshot at
    # *snapshot_index*. Generation 0 means there's no journal yet: *entries*,
    # as older versions of `.raft_state` held them, start generation 1.
    def initialize(@dir : String, @generation : Int64, snapshot_index : Int64, snapshot_term : Int64,
                   entries : Array(Entry) = Array(Entry).new)
      @mem = MemoryLogStore.new(snapshot_index, snapshot_term)
      if @generation.zero?
        entries.each { |e| @mem.append e }
        start_generation
      else
        replay(path(@generation))
        @file = File.open(path(@generation), "a")
      end
      delete_other_generations
    end

    def self.generation_file?(name : String) : Bool
      name.starts_with?(PREFIX)
    end

    def snapshot_index : Int64
      @mem.snapshot_index
    end

    def snapshot_term : Int64
      @mem.snapshot_term
    end

    def size : Int32
      @mem.size
    end

    def entry(index : Int64) : Entry
      @mem.entry(index)
    end

    def append(entry : Entry) : Nil
      index = last_index + 1
      @mem.append entry
      record_size = write_record(RecordType::Append) do |io|
        io.write_bytes index, Format
        Codec.write_entry(io, entry)
      end
      @record_sizes << record_size
      @live_bytes += record_size
    end

    def truncate_from(index : Int64) : Nil
      return if index > last_index
      @mem.truncate_from(index)
      forget_truncated
      @durable_mark.try { |m| @durable_mark = Math.min(m, index - 1) }
      write_record(RecordType::Truncate) { |io| io.write_bytes index, Format }
    end

    # Records that the entries up to *index* are durable, payload included,
    # synced with the next #sync. On opening the journal, entries after the
    # last mark are dropped: the leader logs its own entries before their
    # payload is fsynced, so after a crash those may lack it. Without any mark
    # every entry counts as durable.
    def mark_durable(index : Int64) : Nil
      return if @durable_mark == index
      @durable_mark = index
      write_record(RecordType::Durable) { |io| io.write_bytes index, Format }
    end

    def compact_to(index : Int64, term : Int64) : Nil
      @mem.compact_to(index, term)
      while @record_sizes.size > @mem.size
        @live_bytes -= @record_sizes.shift
      end
      if @journal_bytes > ROTATE_BYTES && @journal_bytes > 2 * @live_bytes
        start_generation
      end
    end

    def reset(index : Int64, term : Int64) : Nil
      @mem.reset(index, term)
      start_generation
    end

    # Whether there's anything #sync has to write
    def pending? : Bool
      @creating || @buffer.size > 0
    end

    # Writes and fsyncs what's queued
    def sync : Nil
      return unless pending?
      if @creating
        file = File.open(path(@generation), "w")
        file.write @buffer.to_slice
        file.fsync
        File.open(@dir, &.fsync)
        @file = file
        @creating = false
      else
        file = @file || raise "Journal #{path(@generation)} isn't open"
        file.write @buffer.to_slice
        file.fsync
      end
      @buffer.clear
    end

    # Deletes the generations replaced since the last call. Only once
    # `.raft_state` names the current one.
    def drop_old_generations : Nil
      return if @old_generations.empty?
      @old_generations.each { |g| File.delete?(path(g)) }
      @old_generations.clear
    end

    def close : Nil
      @file.try &.close
    end

    # Stops counting the records of entries truncated away as live
    private def forget_truncated : Nil
      while @record_sizes.size > @mem.size
        @live_bytes -= @record_sizes.pop
      end
    end

    private def path(generation : Int64) : String
      File.join(@dir, "#{PREFIX}#{generation}")
    end

    # Starts the next generation with the live entries, written at the next
    # #sync
    private def start_generation : Nil
      @file.try &.close
      @file = nil
      @old_generations << @generation unless @creating || @generation.zero?
      @generation += 1
      @creating = true
      @buffer.clear
      @journal_bytes = 0i64
      @record_sizes.clear
      @live_bytes = 0i64
      index = snapshot_index
      @mem.entries.each do |e|
        index += 1
        record_size = write_record(RecordType::Append) do |io|
          io.write_bytes index, Format
          Codec.write_entry(io, e)
        end
        @record_sizes << record_size
        @live_bytes += record_size
      end
      if mark = @durable_mark
        write_record(RecordType::Durable) { |io| io.write_bytes mark, Format }
      end
    end

    # Generations no `.raft_state` names: replaced ones a crash kept from
    # being deleted, or a next one it kept from being named
    private def delete_other_generations : Nil
      Dir.each_child(@dir) do |name|
        next unless JournalLogStore.generation_file?(name)
        next if name == "#{PREFIX}#{@generation}"
        File.delete?(File.join(@dir, name))
      end
    end

    # Queues a record, returns its size
    private def write_record(type : RecordType, & : IO ->) : Int32
      @scratch.clear
      @scratch.write_byte type.value
      yield @scratch
      body = @scratch.to_slice
      raise ArgumentError.new("Journal record of #{body.size} bytes is too large") if body.size > Codec::MAX_FRAME
      @buffer.write_bytes body.size.to_u32, Format
      @buffer.write_bytes Digest::CRC32.checksum(body), Format
      @buffer.write body
      size = HEADER + body.size
      @journal_bytes += size
      size
    end

    private def replay(path : String) : Nil
      bytes = begin
        File.open(path, &.getb_to_end)
      rescue File::NotFoundError
        raise CorruptError.new("The raft state names #{path}, but it's missing")
      end
      pos = 0
      while pos < bytes.size
        size = read_record(bytes, pos) || break
        pos += size
      end
      if pos < bytes.size
        Log.warn { "#{path} ends with an incomplete record at #{pos}, dropping its last #{bytes.size - pos} bytes" }
        File.open(path, "r+") do |f|
          f.truncate(pos)
          f.fsync
        end
      end
      @journal_bytes = pos.to_i64
      if (mark = @durable_mark) && mark < last_index && mark >= snapshot_index
        Log.warn { "#{path} has entries #{mark + 1}..#{last_index} that weren't on disk yet, dropping them" }
        truncate_from(mark + 1)
      end
    end

    # Applies the record at *pos*, returns its size, or nil when it's cut
    # short or doesn't match its checksum
    private def read_record(bytes : Bytes, pos : Int32) : Int32?
      return if bytes.size - pos < HEADER
      length = Format.decode(UInt32, bytes[pos, 4])
      return if length.zero? || length > Codec::MAX_FRAME || bytes.size - pos - HEADER < length
      crc = Format.decode(UInt32, bytes[pos + 4, 4])
      body = bytes[pos + HEADER, length]
      return unless Digest::CRC32.checksum(body) == crc
      size = HEADER + length.to_i32
      apply_record(IO::Memory.new(body, writable: false), size) ? size : nil
    rescue IO::EOFError | IO::Error
      nil
    end

    # False when the record can't be decoded
    private def apply_record(io : IO::Memory, size : Int32) : Bool
      case RecordType.from_value?(io.read_byte || return false)
      when RecordType::Append
        index = io.read_bytes Int64, Format
        entry = Codec.read_entry(io, size)
        if index == last_index + 1
          @mem.append entry
          @record_sizes << size
          @live_bytes += size
        elsif index > snapshot_index # else compacted already
          raise CorruptError.new("Journal appends #{index} after #{last_index}")
        end
      when RecordType::Truncate
        index = io.read_bytes Int64, Format
        if snapshot_index < index <= last_index
          @mem.truncate_from(index)
          forget_truncated
        end
        @durable_mark.try { |m| @durable_mark = Math.min(m, index - 1) }
      when RecordType::Durable
        @durable_mark = io.read_bytes Int64, Format
      else
        raise CorruptError.new("Unknown journal record type")
      end
      true
    end
  end
end
