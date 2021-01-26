require "../segment_position"
require "../client/channel/consumer"

module AvalancheMQ
  class Queue
    class UnackQueue
      record Unack,
        sp : SegmentPosition,
        persistent : Bool,
        consumer : Client::Channel::Consumer?

      @lock = Mutex.new(:checked)

      def initialize(capacity = 8)
        @unacked = Deque(Unack).new(capacity)
      end

      def push(sp : SegmentPosition, persistent : Bool, consumer : Client::Channel::Consumer?)
        @lock.synchronize do
          unacked = @unacked
          unack = Unack.new(sp, persistent, consumer)
          if idx = unacked.bsearch_index { |u| u.sp > sp }
            unacked.insert(idx, unack)
          else
            unacked << unack
          end
        end
      end

      def delete(sp : SegmentPosition)
        @lock.synchronize do
          unacked = @unacked
          if idx = unacked.bsearch_index { |u| u.sp >= sp }
            if unacked[idx].sp == sp
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
    end
  end
end
