require "./journal"
require "./file_applier"

module LavinMQ::Clustering::Raft
  # The Raft log as the Node keeps it: entries journaled, their ops applied
  # to the data dir as they're appended and undone as they're truncated, see
  # FileApplier. Payload isn't journaled, it's in the files once applied.
  class ApplyingLogStore < LogStore
    getter journal : JournalLogStore
    getter applier : FileApplier
    # Entries appended meanwhile are this node's own, as leader: its broker
    # has made the changes, only the tables are updated
    property? local = false

    def initialize(@journal : JournalLogStore, @applier : FileApplier)
    end

    def snapshot_index : Int64
      @journal.snapshot_index
    end

    def snapshot_term : Int64
      @journal.snapshot_term
    end

    def size : Int32
      @journal.size
    end

    def entry(index : Int64) : Entry
      @journal.entry(index)
    end

    def append(entry : Entry) : Nil
      @applier.apply(last_index + 1, entry.data, entry.payload, local: @local)
      @journal.append(entry.payload ? entry.copy_with(payload: nil) : entry)
    end

    def truncate_from(index : Int64) : Nil
      return if index > last_index
      @applier.undo(index, @journal) if index > snapshot_index
      @journal.truncate_from(index)
    end

    def compact_to(index : Int64, term : Int64) : Nil
      @applier.compact(Math.min(index, last_index), @journal)
      @journal.compact_to(index, term)
    end

    def reset(index : Int64, term : Int64) : Nil
      @journal.reset(index, term)
      # The files come with the snapshot, until then they don't match
      @applier.reset(index, @applier.current.dup)
      @applier.needs_snapshot = true
    end
  end
end
