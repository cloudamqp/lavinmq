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
          @unacked << Unack.new(sp, persistent, consumer)
        end
      end

      def delete(sp : SegmentPosition)
        @lock.synchronize do
          if idx = @unacked.index { |u| u.sp == sp }
            @unacked.delete_at(idx)
          end
        end
      end

      def delete(consumer : Client::Channel::Consumer) : Array(SegmentPosition)
        consumer_unacked = Array(SegmentPosition).new(consumer.prefetch_count)
        @lock.synchronize do
          @unacked.delete_if do |unack|
            if unack.consumer == consumer
              consumer_unacked << unack.sp
              true
            end
          end
        end
        consumer_unacked
      end

      def all_segment_positions : Array(SegmentPosition)
        @lock.synchronize do
          @unacked.map &.sp
        end
      end

      def size
        @unacked.size
      end

      def capacity
        @unacked.capacity
      end

      def copy_to(set)
        @lock.synchronize do
          @unacked.each do |sp|
            set << sp
          end
        end
      end
    end
  end
end
