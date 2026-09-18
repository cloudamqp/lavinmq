require "../segment_position"
require "./queue"
require "./consumer"
require "../bool_channel"

module LavinMQ
  module AMQP
    # A channel's outstanding (unacked) deliveries, sorted by delivery tag.
    #
    # Owns the lock, the consumer-unacked counter and the global prefetch
    # capacity signal that depend on the deque, so that every mutation keeps
    # them consistent and no caller has to combine them by hand.
    class UnackedStore
      record Unack,
        tag : UInt64,
        queue : Queue,
        sp : SegmentPosition,
        consumer : AMQP::Consumer?,
        delivered_at : Time::Instant

      getter has_capacity = BoolChannel.new(true)
      getter global_prefetch_count = 0_u16

      @unacked = Deque(Unack).new
      @lock = Mutex.new(:checked)
      # Deliveries to consumers only, basic.get is not counted against the global prefetch window
      @consumer_unacked = Atomic(UInt32).new(0)

      def size : Int32
        @unacked.size
      end

      def has_capacity? : Bool
        return true if @global_prefetch_count.zero?
        @consumer_unacked.get(:relaxed) < @global_prefetch_count
      end

      def global_prefetch_count=(value : UInt16) : Nil
        update_capacity { @global_prefetch_count = value }
      end

      def push(unack : Unack) : Nil
        update_capacity do
          @unacked.push unack
          @consumer_unacked.add(1, :relaxed) if unack.consumer
        end
      end

      def delete(tag : UInt64) : Unack?
        update_capacity do
          # @unacked is always sorted so can do a binary search
          # optimization for acking first unacked
          if @unacked[0]?.try(&.tag) == tag
            remove(@unacked.shift)
          elsif idx = @unacked.bsearch_index { |unack, _| unack.tag >= tag }
            remove(@unacked.delete_at(idx)) if @unacked[idx].tag == tag
          end
        end
      end

      # Removes and yields every delivery up to and including tag, all of them if tag is zero
      def delete_upto(tag : UInt64, & : Unack -> Nil) : Nil
        update_capacity do
          count = if tag.zero?
                    @unacked.size
                  elsif (idx = @unacked.bsearch_index { |unack, _| unack.tag >= tag }) && @unacked[idx].tag == tag
                    idx + 1
                  else
                    0
                  end
          count.times { yield remove(@unacked.shift) }
        end
      end

      # Removes every delivery for which the block returns true
      def reject!(& : Unack -> Bool) : Nil
        update_capacity do
          @unacked.reject! do |unack|
            if yield unack
              remove(unack)
              true
            else
              false
            end
          end
        end
      end

      def includes?(tag : UInt64) : Bool
        @lock.synchronize do
          @unacked.bsearch { |unack| unack.tag >= tag }.try(&.tag) == tag
        end
      end

      def last_tag? : UInt64?
        @lock.synchronize { @unacked.last?.try &.tag }
      end

      # A snapshot, safe to iterate without holding the lock
      def to_a : Array(Unack)
        @lock.synchronize { @unacked.to_a }
      end

      def close : Nil
        @has_capacity.close
      end

      private def remove(unack : Unack) : Unack
        @consumer_unacked.sub(1, :relaxed) if unack.consumer
        unack
      end

      # Runs the block under the lock and flips the capacity signal, in either
      # direction, if the block changed whether there is capacity
      private def update_capacity(&)
        @lock.synchronize do
          had_capacity = has_capacity?
          begin
            yield
          ensure
            now_has_capacity = has_capacity?
            @has_capacity.set(now_has_capacity) if now_has_capacity != had_capacity
          end
        end
      end
    end
  end
end
