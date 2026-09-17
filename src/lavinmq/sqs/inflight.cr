require "random/secure"
require "../min_heap"
require "../segment_position"
require "../amqp/queue"

module LavinMQ
  module SQS
    # Tracks messages that have been handed to SQS clients but not yet deleted
    # (SQS "in flight"). Each is an unacked message in the queue, keyed by an
    # opaque receipt handle. When the visibility timeout of a handle passes
    # without a `DeleteMessage`, the message is requeued so it becomes visible
    # again.
    #
    # Deadlines live in a min-heap with lazy invalidation: changing the
    # visibility of a handle bumps its version and pushes a new heap item, and
    # stale items are skipped when they reach the top.
    class Inflight
      Log           = LavinMQ::Log.for "sqs.inflight"
      HISTORY_LIMIT = 100_000

      class Entry
        getter sp : SegmentPosition
        getter message_id : String
        getter receive_count : UInt32
        getter first_receive_ts : Int64
        property deadline : Time::Instant
        property version = 0_u32

        def initialize(@sp, @message_id, @receive_count, @first_receive_ts, @deadline)
        end
      end

      record HeapItem, deadline : Time::Instant, handle : String, version : UInt32 do
        include Comparable(HeapItem)

        def <=>(other : HeapItem) : Int32
          @deadline <=> other.deadline
        end
      end

      record ReceiveInfo, count : UInt32, first_ts : Int64

      getter queue : AMQP::Queue

      def initialize(@queue : AMQP::Queue)
        @entries = Hash(String, Entry).new
        @heap = MinHeap(HeapItem).new
        # Receive count and first receive time per message, kept while the
        # message is in the queue (also between receives) so
        # ApproximateReceiveCount survives a visibility timeout.
        @history = Hash(SegmentPosition, ReceiveInfo).new
        @lock = Mutex.new
        @wakeup = Channel(Nil).new(1)
        @closed = false
        spawn(name: "SQS visibility timeouts #{@queue.name}") { expire_loop }
      end

      # Registers a message just handed to a client and returns its receipt handle.
      def add(sp : SegmentPosition, message_id : String, visibility_timeout : Time::Span, now_ms : Int64) : Tuple(String, Entry)
        handle = Random::Secure.urlsafe_base64(32)
        deadline = Time.instant + visibility_timeout
        entry = @lock.synchronize do
          info = @history[sp]?
          count = (info.try(&.count) || 0_u32) + 1
          first_ts = info.try(&.first_ts) || now_ms
          @history.shift if @history.size >= HISTORY_LIMIT && !@history.has_key?(sp)
          @history[sp] = ReceiveInfo.new(count, first_ts)
          new_entry = Entry.new(sp, message_id, count, first_ts, deadline)
          @entries[handle] = new_entry
          @heap.push HeapItem.new(deadline, handle, new_entry.version)
          new_entry
        end
        @wakeup.try_send? nil
        {handle, entry}
      end

      # Forgets the handle and returns its segment position, or nil if the
      # handle is unknown (never issued, already deleted or expired).
      def delete(handle : String) : SegmentPosition?
        @lock.synchronize do
          entry = @entries.delete(handle) || return
          @history.delete(entry.sp)
          entry.sp
        end
      end

      # Forgets the handle but keeps the receive history, for a message that
      # is made visible again (visibility timeout set to 0) rather than deleted.
      def release(handle : String) : SegmentPosition?
        @lock.synchronize do
          @entries.delete(handle).try(&.sp)
        end
      end

      def change_visibility(handle : String, visibility_timeout : Time::Span) : Bool
        @lock.synchronize do
          entry = @entries[handle]? || return false
          entry.version += 1
          entry.deadline = Time.instant + visibility_timeout
          @heap.push HeapItem.new(entry.deadline, handle, entry.version)
        end
        @wakeup.try_send? nil
        true
      end

      def includes?(handle : String) : Bool
        @lock.synchronize { @entries.has_key?(handle) }
      end

      def size : Int32
        @lock.synchronize { @entries.size }
      end

      def close : Nil
        @closed = true
        @wakeup.close
      end

      private def expire_loop : Nil
        until @closed || @queue.closed?
          if wait = next_wait
            if wait <= Time::Span.zero
              expire_due
              next
            end
            select
            when @wakeup.receive
            when timeout wait
            end
          else
            @wakeup.receive
          end
        end
      rescue Channel::ClosedError
      end

      # Time until the earliest valid deadline, or nil when nothing is in flight
      private def next_wait : Time::Span?
        @lock.synchronize do
          while item = @heap.first?
            entry = @entries[item.handle]?
            if entry.nil? || entry.version != item.version
              @heap.shift? # stale item
              next
            end
            return item.deadline - Time.instant
          end
        end
        nil
      end

      private def expire_due : Nil
        now = Time.instant
        expired = Array(Entry).new
        @lock.synchronize do
          while item = @heap.first?
            break if item.deadline > now
            @heap.shift?
            entry = @entries[item.handle]? || next
            next if entry.version != item.version
            @entries.delete(item.handle)
            expired << entry
          end
        end
        expired.each do |entry|
          @queue.reject(entry.sp, true)
        rescue ex
          Log.warn(exception: ex) { "Could not requeue #{entry.sp} in #{@queue.name} after visibility timeout" }
        end
      end
    end
  end
end
