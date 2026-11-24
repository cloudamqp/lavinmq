require "../segment_position"
require "../bool_channel"
require "../rough_time"
require "./queue"
require "./consumer"

module LavinMQ
  module AMQP
    # A channel's unacked deliveries, sorted by delivery tag.
    #
    # Owns the lock and the global prefetch window that depend on them, so
    # that every change keeps them consistent. Blocks are called while the
    # lock is held, so they can't call back into the store, except #reserve.
    class UnackedStore
      record Unack,
        tag : UInt64,
        queue : Queue,
        sp : SegmentPosition,
        consumer : AMQP::Consumer?,
        delivered_at : Time::Instant

      include Enumerable(Unack)

      getter has_capacity = BoolChannel.new(true)
      getter global_prefetch_count = 0_u16
      @unacked = Deque(Unack).new
      @lock = Mutex.new(:checked)
      # Deliveries that count against the global prefetch: those to consumers,
      # not basic.get, and the reserved ones
      @consumer_count = Atomic(Int32).new(0)

      def each(& : Unack ->) : Nil
        @lock.synchronize { @unacked.each { |unack| yield unack } }
      end

      def size : Int32
        @unacked.size
      end

      def has_capacity? : Bool
        @global_prefetch_count.zero? || @consumer_count.get(:relaxed) < @global_prefetch_count
      end

      def global_prefetch_count=(value : UInt16)
        update_capacity { @global_prefetch_count = value }
      end

      # The block returns the delivery tag, or raises to not record the
      # delivery. Allocate the tag in the block, so that concurrent pushes
      # can't record their tags out of order.
      def push(queue : Queue, sp : SegmentPosition, consumer : AMQP::Consumer?, & : -> UInt64) : UInt64
        update_capacity do
          tag = yield
          @unacked.push Unack.new(tag, queue, sp, consumer, RoughTime.instant)
          @consumer_count.add(1, :relaxed) if consumer
          tag
        end
      end

      # Puts back a removed delivery, if the block returns true
      def restore(unack : Unack, & : -> Bool) : Bool
        update_capacity do
          return false unless yield
          idx = @unacked.bsearch_index { |u, _| u.tag >= unack.tag } || @unacked.size
          @unacked.insert(idx, unack)
          @consumer_count.add(1, :relaxed) if unack.consumer
          true
        end
      end

      def delete(tag : UInt64) : Unack?
        update_capacity do
          # @unacked is always sorted so can do a binary search
          # optimization for acking first unacked
          if @unacked[0]?.try(&.tag) == tag
            removed(@unacked.shift)
          elsif idx = @unacked.bsearch_index { |unack, _| unack.tag >= tag }
            removed(@unacked.delete_at(idx)) if @unacked[idx].tag == tag
          end
        end
      end

      # Removes and yields the deliveries up to and including tag, all of them if tag is zero
      def delete_upto(tag : UInt64, & : Unack ->) : Nil
        update_capacity do
          count = @unacked.size
          unless tag.zero?
            idx = @unacked.bsearch_index { |unack, _| unack.tag >= tag }
            return unless idx && @unacked[idx].tag == tag
            count = idx + 1
          end
          count.times { yield removed(@unacked.shift) }
        end
      end

      # Removes the deliveries the block returns true for
      def reject!(& : Unack -> Bool) : Nil
        update_capacity do
          @unacked.reject! do |unack|
            next false unless yield unack
            removed(unack)
            true
          end
        end
      end

      # Keeps counting the delivery that a #reject! block is removing against
      # the global prefetch, until #release
      def reserve : Nil
        @consumer_count.add(1, :relaxed)
      end

      def release(count : Int32) : Nil
        update_capacity { @consumer_count.sub(count, :relaxed) }
      end

      def includes_tag?(tag : UInt64) : Bool
        @lock.synchronize do
          @unacked.bsearch { |unack| unack.tag >= tag }.try(&.tag) == tag
        end
      end

      def last_tag? : UInt64?
        @lock.synchronize { @unacked.last?.try &.tag }
      end

      def close : Nil
        @has_capacity.close
      end

      private def removed(unack : Unack) : Unack
        @consumer_count.sub(1, :relaxed) if unack.consumer
        unack
      end

      # Flips the capacity signal if the block changed whether there's capacity
      private def update_capacity(&)
        @lock.synchronize do
          had_capacity = has_capacity?
          begin
            yield
          ensure
            capacity = has_capacity?
            @has_capacity.set(capacity) unless capacity == had_capacity
          end
        end
      end
    end
  end
end
