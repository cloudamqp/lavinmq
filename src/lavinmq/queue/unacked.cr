require "../segment_position"
require "../client/channel/consumer"

module LavinMQ
  class Queue
    class UnackQueue
      record Unack,
        sp : SegmentPosition,
        consumer : Client::Channel::Consumer?

      @lock = Mutex.new(:checked)
      getter bytesize = 0u64

      def initialize(capacity = 8)
        @unacked = Deque(Unack).new(capacity)
      end

      def push(sp : SegmentPosition, consumer : Client::Channel::Consumer?)
        @lock.synchronize do
          unacked = @unacked
          unack = Unack.new(sp, consumer)
          if idx = unacked.bsearch_index { |u| u.sp > sp }
            unacked.insert(idx, unack)
          else
            unacked << unack
          end
          @bytesize += sp.bytesize
        end
      end

      def delete(sp : SegmentPosition)
        @lock.synchronize do
          unacked = @unacked
          if idx = unacked.bsearch_index { |u| u.sp >= sp }
            if unacked[idx].sp == sp
              @bytesize -= sp.bytesize
              unacked.delete_at(idx)
            end
          end
        end
      end

      def delete(consumer : Client::Channel::Consumer) : Array(SegmentPosition)
        consumer_unacked = Array(SegmentPosition).new(Math.max(consumer.prefetch_count, 16))
        @lock.synchronize do
          @unacked.reject! do |unack|
            if unack.consumer == consumer
              consumer_unacked << unack.sp
              @bytesize -= unack.sp.bytesize
              true
            end
          end
        end
        consumer_unacked
      end

      def size
        @unacked.size
      end

      def [](index) : Unack
        @unacked[index]
      end

      def []?(index) : Unack?
        @unacked[index]?
      end

      def sum(&blk : Unack -> _) : UInt64
        @unacked.sum(0_u64, &blk)
      end

      def avg_bytesize
        return 0u64 if @unacked.size.zero?
        @bytesize // @unacked.size
      end

      # expensive calculation used for unacked queue snapshot
      def max_bytesize(&blk : Unack -> _) : UInt32
        return 0u32 if @unacked.size.zero?
        @unacked.max_of(&blk)
      end

      # expensive calculation used for unacked queue snapshot
      def min_bytesize(&blk : Unack -> _) : UInt32
        return 0u32 if @unacked.size.zero?
        @unacked.min_of(&blk)
      end

      def capacity
        @unacked.capacity
      end

      def locked_each
        @lock.synchronize do
          yield @unacked.each
        end
      end

      def each_sp(&blk)
        @lock.synchronize do
          @unacked.each { |unack| yield unack.sp }
        end
      end

      def compact
        @lock.synchronize do
          @unacked = Deque(Unack).new(@unacked.size) { |i| @unacked[i] }
        end
      end

      def lock
        @lock.lock
      end

      def unlock
        @lock.unlock
      end

      def purge
        @lock.synchronize do
          s = @unacked.size
          @unacked.clear
          @bytesize = 0u64
          s
        end
      end
    end
  end
end
