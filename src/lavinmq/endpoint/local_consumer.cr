require "../amqp/queue_consumer"
require "../logger"
require "./session"

module LavinMQ
  module Endpoint
    # A consumer of a LocalSession: an AMQP::QueueConsumer, so it waits for
    # the same things as an AMQP consumer before taking a message (prefetch,
    # single active consumer, priority consumers, a paused queue), and is
    # counted like one for consumer counts, exclusivity and auto-delete.
    #
    # It delivers to a block instead of a channel, on the fiber of
    # LocalSession#consume: the deliver loop runs there (see #run) rather
    # than on a fiber of its own. Deliveries are read with Queue#consume_get,
    # whose segment lease keeps the message mapped while the block handles
    # it, so the session hands out borrowed bodies without copying. Because
    # of the lease the loop doesn't need to be part of the queue's
    # deliver_loop_wg: a destination that is slow to accept a message
    # doesn't hold up Queue#close.
    class LocalConsumer < AMQP::QueueConsumer
      Log = LavinMQ::Log.for "endpoint.consumer"
      @handler : Proc(Delivery, Nil)?

      def initialize(@session : LocalSession, queue : AMQP::Queue, tag : String,
                     no_ack : Bool, exclusive : Bool, prefetch_count : UInt16)
        metadata = ::Log::Metadata.new(nil, {vhost: queue.vhost.name, queue: queue.name, consumer: tag})
        super(queue, tag, no_ack, exclusive, 0, prefetch_count, true, Logger.new(Log, metadata))
      end

      # Delivers messages to the block until cancelled, or the queue is
      # closed or deleted
      def run(&blk : Delivery -> Nil) : Nil
        @handler = blk
        deliver_loop
      rescue ex : HandlerError
        raise ex.error
      end

      # What the consume block raised, wrapped to pass the deliver loop's
      # rescue: it treats a closed channel or an IO error as its own
      # shutdown, which the block's (a shovel's remote destination losing
      # its connection) is not
      private class HandlerError < Exception
        getter error : Exception

        def initialize(@error)
          super(@error.message)
        end
      end

      # The deliver loop is #run's, on the consuming fiber, and it wakes on
      # the queue's signals by itself: there's never one to spawn
      def ensure_deliver_loop
      end

      # Called by the queue when it's closed or deleted, and by the session
      def cancel
        close
      end

      # A loop on the consuming fiber can't exit while idle, it must keep
      # waiting until cancelled
      private def exit_when_idle? : Bool
        false
      end

      # The tag is registered atomically with the session's close: nil once
      # the session (or this consumer) is closed, and the message is then
      # returned to the queue, not delivered
      def deliver(msg, sp, redelivered = false, recover = false)
        handler = @handler || raise ClosedError.new
        tag = @session.next_delivery_tag(self, sp) unless @closed
        unless tag
          @queue.requeue(sp) unless @no_ack # a no-ack get requeues it itself
          raise ClosedError.new
        end
        track_unacked unless @no_ack
        begin
          handler.call Delivery.new(tag, msg.exchange_name, msg.routing_key,
            msg.properties, msg.body, redelivered)
        rescue ex
          raise HandlerError.new(ex)
        end
      end

      # A delivery of this consumer was acked or rejected, in the order an
      # AMQP channel settles them
      def settled(sp : SegmentPosition, ack : Bool, requeue : Bool) : Nil
        if ack
          ack(sp)
          @queue.ack(sp)
        else
          reject(sp, requeue)
          @queue.reject(sp, requeue)
        end
      end

      # A delivery of this consumer the session returns to the queue when it
      # closes. Like an AMQP channel's close, a broker requeue: right away,
      # never through the queue's delayed retry, and not counted as a reject.
      def returned(sp : SegmentPosition) : Nil
        reject(sp, requeue: true)
        @queue.requeue(sp)
      end

      # No channel-wide prefetch, flow or stats: the session has none
      private def global_capacity_signal : ::Channel(Nil)?
      end

      private def global_prefetch_full? : Bool
        false
      end

      private def count_delivery(redelivered : Bool) : Nil
      end

      def flush
        Fiber.yield
      end

      private def channel_details
        {
          peer_host:       nil,
          peer_port:       nil,
          connection_name: @session.name,
          user:            nil,
          number:          0,
          name:            @session.name,
        }
      end
    end
  end
end
