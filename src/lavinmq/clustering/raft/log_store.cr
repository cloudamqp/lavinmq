require "./messages"

module LavinMQ::Clustering::Raft
  # The entries of the log after the snapshot, by their index in the log.
  # Entries up to and including `snapshot_index` have been compacted away,
  # Core keeps what they amounted to. Pure, see Core: it's up to the owner
  # to persist what's appended.
  abstract class LogStore
    # The index of the last compacted entry, 0 when nothing is
    abstract def snapshot_index : Int64
    # The term of the last compacted entry, 0 when nothing is
    abstract def snapshot_term : Int64
    abstract def size : Int32

    def last_index : Int64
      snapshot_index + size
    end

    def empty? : Bool
      size.zero?
    end

    # The entry at *index*, which must be after the snapshot and at most
    # last_index
    abstract def entry(index : Int64) : Entry

    abstract def append(entry : Entry) : Nil

    # Drops the entry at *index* and everything after it
    abstract def truncate_from(index : Int64) : Nil

    # Drops the entries up to and including *index*, that last one has *term*.
    # Everything is dropped when *index* is beyond the last entry.
    abstract def compact_to(index : Int64, term : Int64) : Nil

    # Drops every entry and starts over after *index*, which has *term*, as
    # when a snapshot is installed
    abstract def reset(index : Int64, term : Int64) : Nil

    # The term of the entry at *index*: the snapshot's term at its index, 0
    # before it (compacted, unknown) or after the last entry
    def term_at(index : Int64) : Int64
      return snapshot_term if index == snapshot_index
      return 0i64 if index < snapshot_index || index > last_index
      entry(index).term
    end

    # Up to *max* entries from *from* on, fewer when the log ends first
    def slice(from : Int64, max : Int32 = Int32::MAX) : Array(Entry)
      from = snapshot_index + 1 if from <= snapshot_index
      count = Math.min(max.to_i64, last_index - from + 1)
      return Array(Entry).new if count <= 0
      Array(Entry).new(count.to_i) { |i| entry(from + i) }
    end

    # The entries from the last one back to the first after the snapshot
    def reverse_each(& : Entry ->) : Nil
      index = last_index
      while index > snapshot_index
        yield entry(index)
        index -= 1
      end
    end

    # Every entry after the snapshot
    def entries : Array(Entry)
      slice(snapshot_index + 1)
    end
  end

  # A LogStore in memory
  class MemoryLogStore < LogStore
    getter snapshot_index : Int64
    getter snapshot_term : Int64
    @entries : Array(Entry)

    def initialize(@snapshot_index = 0i64, @snapshot_term = 0i64, entries : Array(Entry) = Array(Entry).new)
      @entries = entries.dup
    end

    def size : Int32
      @entries.size
    end

    def entry(index : Int64) : Entry
      @entries[offset(index)]
    end

    def append(entry : Entry) : Nil
      @entries << entry
    end

    def truncate_from(index : Int64) : Nil
      keep = offset(index)
      return if keep >= @entries.size
      raise ArgumentError.new("Can't truncate from #{index}, the snapshot ends at #{@snapshot_index}") if keep < 0
      @entries.truncate(0, keep)
    end

    def compact_to(index : Int64, term : Int64) : Nil
      return if index <= @snapshot_index
      if index >= last_index
        @entries.clear
      else
        @entries.shift(offset(index) + 1)
      end
      @snapshot_index = index
      @snapshot_term = term
    end

    def reset(index : Int64, term : Int64) : Nil
      @entries.clear
      @snapshot_index = index
      @snapshot_term = term
    end

    private def offset(index : Int64) : Int32
      (index - @snapshot_index - 1).to_i
    end
  end
end
