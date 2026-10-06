require "../client/channel/consumer"
require "../bool_channel"
require "../config"
require "./session"

module LavinMQ
  module Endpoint
    # A consumer of a LocalSession, registered on the queue like an AMQP
    # consumer so consumer counts, exclusivity, single active consumer,
    # auto-delete and "has consumers" checks all see it.
    #
    # It has no fiber of its own: LocalSession#consume runs #run on the calling
    # fiber. Deliveries are read with Queue#consume_get, whose segment lease
    # keeps the message mapped while the block handles it, so the session can
    # hand out borrowed bodies without copying. Because of the lease the loop
    # doesn't need to be part of the queue's deliver_loop_wg: a destination
    # that is slow to accept a message doesn't hold up Queue#close.
    class LocalConsumer < Client::Channel::Consumer
      include SortableJSON

      getter tag : String
      getter queue : AMQP::Queue
      getter priority = 0
      getter? exclusive : Bool
      getter? no_ack : Bool
      getter prefetch_count : UInt16
      getter has_capacity = BoolChannel.new(true)
      getter? closed = false
      @unacked = Atomic(UInt32).new(0_u32)
      # Closed when the consumer is cancelled; wakes #run
      @cancelled = ::Channel(Nil).new
      # Set when the queue may have something for us; wakes #run
      @wakeup = ::Channel(Nil).new(1)

      def initialize(@session : LocalSession, @queue : AMQP::Queue, @tag : String,
                     @no_ack : Bool, @exclusive : Bool, @prefetch_count : UInt16)
      end

      def unacked : UInt32
        @unacked.get(:relaxed)
      end

      def accepts? : Bool
        return false if @closed
        @no_ack || @prefetch_count.zero? || unacked < @prefetch_count
      end

      # Called by the queue when messages become available
      def ensure_deliver_loop
        @wakeup.try_send?(nil) unless @closed
      end

      # Called by the queue when it's closed or deleted, and by the session
      def cancel
        return if @closed
        @closed = true
        @cancelled.close
        @has_capacity.close
      end

      # A delivery of this consumer was acked or rejected
      def settled(sp : SegmentPosition, ack : Bool, requeue : Bool) : Nil
        decrement_unacked
        if ack
          @queue.ack(sp)
        else
          @queue.reject(sp, requeue)
        end
      end

      protected def decrement_unacked : Nil
        unacked = @unacked.sub(1, :relaxed)
        @has_capacity.set(true) if unacked == @prefetch_count
      rescue ::Channel::ClosedError
      end

      protected def increment_unacked : Nil
        return if @no_ack
        unacked = @unacked.add(1, :relaxed) + 1
        @has_capacity.set(false) if unacked == @prefetch_count
      rescue ::Channel::ClosedError
      end

      # Delivers messages to the block until cancelled
      def run(&blk : Delivery -> Nil) : Nil
        delivered_bytes = 0
        yield_each = Config.instance.yield_each_delivered_bytes
        loop do
          break unless wait_until_ready
          get do |env|
            sp = env.segment_position
            # Cancelled while the message was being fetched: give it back
            if @closed
              raise Cancelled.new if @no_ack # the queue requeues it
              @queue.reject(sp, true)
              next
            end
            increment_unacked
            delivered_bytes &+= sp.bytesize
            msg = env.message
            tag = @session.next_delivery_tag(self, sp)
            blk.call Delivery.new(tag, msg.exchange_name, msg.routing_key,
              msg.properties, msg.body, env.redelivered)
          end
          if delivered_bytes > yield_each
            delivered_bytes = 0
            Fiber.yield
          end
        end
      rescue Cancelled | AMQP::Queue::ClosedError | MessageStore::ClosedError
        # Cancelled, or the queue was closed or deleted while we read from it
      end

      private class Cancelled < Exception; end

      protected def get(& : Envelope -> Nil) : Bool
        @queue.consume_get(@no_ack) { |env| yield env }
      end

      protected def queue_ready? : Bool
        !@queue.empty?
      end

      # Waits for capacity, our turn as single active consumer, a ready queue
      # that isn't paused. Returns false once cancelled.
      private def wait_until_ready : Bool
        loop do
          return false if @closed
          if !@no_ack && @prefetch_count > 0 && unacked >= @prefetch_count
            select
            when @has_capacity.when_true.receive?
            when @cancelled.receive?
            end
            next
          end
          if (sac = @queue.single_active_consumer) && sac != self
            select
            when @queue.single_active_consumer_change.receive?
            when @cancelled.receive?
            end
            next
          end
          if @queue.state.paused?
            select
            when @queue.paused.when_false.receive?
            when @cancelled.receive?
            end
            next
          end
          unless queue_ready?
            select
            when @wakeup.receive?
            when @queue.empty.when_false.receive?
            when @cancelled.receive?
            end
            next
          end
          return true
        end
      end

      def details_tuple
        {
          queue: {
            name:  @queue.name,
            vhost: @queue.vhost.name,
          },
          consumer_tag:    @tag,
          exclusive:       @exclusive,
          ack_required:    !@no_ack,
          prefetch_count:  @prefetch_count,
          priority:        @priority,
          channel_details: {
            peer_host:       nil,
            peer_port:       nil,
            connection_name: @session.name,
            user:            nil,
            number:          0,
            name:            @session.name,
          },
        }
      end
    end
  end
end
