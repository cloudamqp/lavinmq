require "../mfile"

module LavinMQ
  module Clustering
    # The leader's ordered log of replicated file operations.
    #
    # Every write that must reach the followers is appended as one entry and
    # gets a log sequence number (lsn). Each synced follower has a sender fiber
    # that tails the journal from the lsn it was marked synced at and encodes
    # the entries for the wire, so a publish only pays a mutex and a struct
    # copy here; compression and socket writes happen in the senders.
    #
    # The journal is a fixed-capacity ring. Appends never block: when the
    # ring is full the oldest entry is evicted, and a sender that has not
    # read it yet fell behind the journal — it disconnects and the follower
    # resyncs. A follower joins at the head: the entries before its cut reach
    # it through its full sync, so appends made while nobody subscribes hold
    # no lease.
    #
    # An Append entry doesn't copy its payload: the bytes are read from the
    # segment's mmap when sent. To keep the mapping alive until then (the
    # segment may roll over or be deleted meanwhile) the entry holds an MFile
    # lease while any subscriber may still read it, and #read hands the sender
    # a lease of its own for the time it takes to encode the entry.
    class Journal
      Log = LavinMQ::Log.for "clustering.journal"

      # Number of entries retained. A follower further behind than this
      # disconnects and resyncs.
      CAPACITY = 1 << 18

      enum Op : UInt8
        Append      # len bytes at offset in mfile's mmap
        AppendValue # a 4-byte value (schema version, ack position) at offset
        AppendBytes # a copied buffer at offset
        Replace     # the whole file, copied; len 0 is the delete marker
        Delete
        Fsync # the path, or "" for the whole data dir
      end

      struct Entry
        getter lsn : UInt64
        getter op : Op
        # Relative to the data dir
        getter path : String
        # Where the appended bytes are in the file; the follower's join cut
        # decides from it which bytes it already got from its full sync
        getter offset : Int64
        getter len : Int64
        getter mfile : MFile?
        getter bytes : Bytes
        getter value : UInt32

        def initialize(@lsn, @op, @path, @offset = 0i64, @len = 0i64,
                       @mfile = nil, @bytes = Bytes.empty, @value = 0u32)
        end

        # The bytes to send. Only valid while the mfile is leased (see
        # Journal#read).
        def payload : Bytes
          if mfile = @mfile
            mfile.to_slice(@offset, @len)
          else
            @bytes
          end
        end
      end

      # A sender tailing the journal. `journal_position` is the next lsn it
      # will read: the journal keeps the entries from there on leased for it.
      module Subscriber
        abstract def wake : Nil
        abstract def journal_position : UInt64
        abstract def journal_position=(lsn : UInt64)
      end

      # Lsn 0 is never assigned: "nothing journaled yet" (see #last_lsn)
      @head = 1u64
      @tail = 1u64
      # Entries below this had their lease released (or never took one)
      @released = 1u64
      @lock = Mutex.new(:unchecked)
      @subscribers = Array(Subscriber).new(4)

      def initialize(@capacity : Int32 = CAPACITY)
        @ring = Slice(Entry).new(@capacity, Entry.new(0u64, Op::Delete, ""))
      end

      # The next lsn to be assigned
      def head : UInt64
        @lock.synchronize { @head }
      end

      # The oldest lsn still in the journal
      def tail : UInt64
        @lock.synchronize { @tail }
      end

      # The lsn of the most recent entry, 0 if none
      def last_lsn : UInt64
        @lock.synchronize { @head - 1 }
      end

      def size : Int32
        @lock.synchronize { (@head - @tail).to_i32 }
      end

      def append_mfile(path : String, mfile : MFile, offset : Int64, len : Int64) : UInt64
        append Entry.new(0u64, Op::Append, path, offset, len, mfile: mfile)
      end

      def append_value(path : String, value : UInt32, offset : Int64) : UInt64
        append Entry.new(0u64, Op::AppendValue, path, offset, sizeof(UInt32).to_i64, value: value)
      end

      def append_bytes(path : String, bytes : Bytes, offset : Int64) : UInt64
        append Entry.new(0u64, Op::AppendBytes, path, offset, bytes.bytesize.to_i64, bytes: bytes)
      end

      def replace(path : String, bytes : Bytes) : UInt64
        append Entry.new(0u64, Op::Replace, path, len: bytes.bytesize.to_i64, bytes: bytes)
      end

      def delete(path : String) : UInt64
        append Entry.new(0u64, Op::Delete, path)
      end

      def fsync(path : String) : UInt64
        append Entry.new(0u64, Op::Fsync, path)
      end

      # Assigns the entry its lsn and wakes the subscribers. Never blocks.
      private def append(entry : Entry) : UInt64
        @lock.synchronize do
          lsn = @head
          evict_oldest if @head - @tail == @capacity
          @ring[(lsn % @capacity).to_i32] = Entry.new(lsn, entry.op, entry.path, entry.offset, entry.len,
            entry.mfile, entry.bytes, entry.value)
          @head = lsn + 1
          if @subscribers.empty?
            # Nobody will read it (a joining follower starts at the head), so
            # no lease is taken and the entry is released right away. Earlier
            # entries were released when the last subscriber left.
            @released = @head
          else
            entry.mfile.try &.lease
            @subscribers.each &.wake
          end
          lsn
        end
      end

      # Caller holds @lock
      private def evict_oldest : Nil
        release_entry(@ring[(@tail % @capacity).to_i32]) if @tail >= @released
        @tail += 1
        @released = @tail if @released < @tail
      end

      # Caller holds @lock
      private def release_entry(entry : Entry) : Nil
        entry.mfile.try &.release_lease
      end

      # The entry at lsn, with its mfile leased for the caller, who must
      # `release_lease` it after use. Nil once the entry is evicted or
      # released (the caller fell behind, see #behind?) or not yet appended.
      def read(lsn : UInt64) : Entry?
        @lock.synchronize do
          return if lsn < @released || lsn >= @head
          entry = @ring[(lsn % @capacity).to_i32]
          entry.mfile.try &.lease
          entry
        end
      end

      # True when the entry at lsn is gone: a sender at that position can't
      # continue and its follower must resync
      def behind?(lsn : UInt64) : Bool
        @lock.synchronize { lsn < @released }
      end

      # Entries appended from now on are kept for the subscriber, which
      # starts at the current head
      def subscribe(subscriber : Subscriber) : Nil
        @lock.synchronize do
          subscriber.journal_position = @head
          @subscribers << subscriber unless @subscribers.includes?(subscriber)
        end
      end

      def unsubscribe(subscriber : Subscriber) : Nil
        @lock.synchronize do
          @subscribers.delete(subscriber)
          release_unneeded_locked
        end
      end

      # Release the leases of every entry all subscribers have passed. Called
      # by senders after each batch.
      def release_unneeded : Nil
        @lock.synchronize { release_unneeded_locked }
      end

      private def release_unneeded_locked : Nil
        upto = @head
        @subscribers.each do |s|
          pos = s.journal_position
          upto = pos if pos < upto
        end
        while @released < upto
          release_entry(@ring[(@released % @capacity).to_i32]) if @released >= @tail
          @released += 1
        end
      end

      def subscribers : Int32
        @lock.synchronize { @subscribers.size }
      end
    end
  end
end
