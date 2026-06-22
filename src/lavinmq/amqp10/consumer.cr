require "../client/channel/consumer"
require "../bool_channel"
require "../segment_position"
require "./frames"
require "./messaging"

module LavinMQ
  module AMQP10
    # A consuming (sender) link: the broker sends messages from a queue to the
    # client. Delivery is paced by AMQP 1.0 link credit (granted by the client's
    # `flow`) rather than AMQP 0-9-1 prefetch. Reuses the queue's deliver-loop
    # machinery (`ensure_deliver_loop`, `consume_get`) like `AMQP::Consumer`.
    class Consumer < LavinMQ::Client::Channel::Consumer
      Log = LavinMQ::Log.for "amqp10.consumer"

      getter tag : String
      getter handle : UInt32
      getter queue : LavinMQ::AMQP::Queue
      getter? closed = false
      getter has_capacity : BoolChannel
      @credit = Atomic(UInt32).new(0_u32)
      @delivery_count : UInt32
      @next_delivery_id = Atomic(UInt32).new(0_u32)
      @unacked = Atomic(UInt32).new(0_u32)
      @deliver_loop_running = Atomic(Bool).new(false)
      @notify_closed = ::Channel(Nil).new
      # delivery-id -> queue segment position, for settlement
      @unsettled = Hash(UInt32, SegmentPosition).new
      @unsettled_lock = Mutex.new

      def initialize(@client : Client, @queue : LavinMQ::AMQP::Queue, @handle : UInt32,
                     @tag : String, @no_ack : Bool, initial_delivery_count : UInt32)
        @delivery_count = initial_delivery_count
        @has_capacity = BoolChannel.new(false)
      end

      def no_ack? : Bool
        @no_ack
      end

      def priority : Int32
        0
      end

      def exclusive? : Bool
        false
      end

      def unacked : UInt32
        @unacked.get(:relaxed)
      end

      # Client granted link credit via a flow performative. Per the spec a sender
      # may send while its delivery-count < flow.delivery-count + flow.link-credit,
      # so the available credit is rebased against our own delivery-count — this
      # is what keeps credit correct across link recovery / drain when the client
      # advances its delivery-count.
      def grant_credit(link_credit : UInt32, delivery_count : UInt32?) : Nil
        credit = if dc = delivery_count
                   limit = dc &+ link_credit
                   limit > @delivery_count ? limit &- @delivery_count : 0_u32
                 else
                   link_credit
                 end
        @credit.set(credit)
        @has_capacity.set(credit > 0)
        ensure_deliver_loop if credit > 0
      end

      def accepts? : Bool
        @credit.get(:relaxed) > 0
      end

      def ensure_deliver_loop : Nil
        return if @closed
        return if @deliver_loop_running.swap(true, :acquire_release)
        @queue.deliver_loop_wg.spawn(name: "AMQP10::Consumer #{@queue.name}") { deliver_loop }
      end

      private def deliver_loop
        loop do
          wait_for_credit
          case wait_for_queue_ready
          when :timeout
            @deliver_loop_running.set(false, :release)
            ensure_deliver_loop unless @queue.empty?
            return
          when :closed
            @deliver_loop_running.set(false, :release)
            return
          end
          @queue.consume_get(@no_ack) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
        end
      rescue ex : LavinMQ::AMQP::Queue::ClosedError | ::Channel::ClosedError | IO::Error
        @deliver_loop_running.set(false, :release)
      rescue ex
        @deliver_loop_running.set(false, :release)
        Log.warn(exception: ex) { "deliver loop exited" }
      end

      private def wait_for_credit : Nil
        until @credit.get(:relaxed) > 0
          @client.flush
          select
          when @has_capacity.when_true.receive
          when @notify_closed.receive
            raise ::Channel::ClosedError.new
          end
        end
      end

      private def wait_for_queue_ready : ::Symbol
        return :ready unless @queue.empty?
        @client.flush
        select
        when @queue.empty.when_false.receive
          :ready
        when @notify_closed.receive
          :closed
        when timeout Config.instance.deliver_loop_idle_timeout
          :timeout
        end
      end

      def deliver(msg, sp : SegmentPosition, redelivered = false, recover = false) : Nil
        delivery_id = @next_delivery_id.add(1, :relaxed)
        unless @no_ack
          @unacked.add(1, :relaxed)
          @unsettled_lock.synchronize { @unsettled[delivery_id] = sp }
        end
        @client.deliver_message(@handle, delivery_id, @no_ack, msg)
        # one credit consumed per transfer
        remaining = @credit.sub(1, :relaxed) - 1
        @delivery_count &+= 1
        @has_capacity.set(false) if remaining == 0
      end

      # Apply a client disposition for a delivery to the queue.
      def settle(delivery_id : UInt32, outcome : DeliveryState::Outcome) : Nil
        sp = @unsettled_lock.synchronize { @unsettled.delete(delivery_id) }
        return unless sp
        @unacked.sub(1, :relaxed)
        case outcome
        when .accepted? then @queue.ack(sp)
        when .rejected? then @queue.reject(sp, requeue: false)
        else                 @queue.reject(sp, requeue: true) # released / modified
        end
      end

      # ---- interface methods called polymorphically on the base Consumer ----
      # AMQP 1.0 paces delivery with link credit, not prefetch/flow, so these are
      # no-ops; settlement is driven by `settle` from dispositions instead.

      def flow(active : Bool) : Nil
      end

      def ack(sp) : Nil
      end

      def reject(sp, requeue = false) : Nil
      end

      def prefetch_count : UInt16
        0_u16
      end

      def prefetch_count=(value : UInt16) : Nil
      end

      def cancel : Nil
        close
      end

      def close : Nil
        return if @closed
        @closed = true
        @notify_closed.close
        @has_capacity.close
        @queue.rm_consumer(self)
        # Requeue anything still unsettled.
        @unsettled_lock.synchronize do
          @unsettled.each_value { |sp| @queue.reject(sp, requeue: true) }
          @unsettled.clear
        end
      end

      def unacked_messages
        [] of LavinMQ::AMQP::Queue::UnackedMessage
      end

      def details_tuple
        {
          queue: {
            name:  @queue.name,
            vhost: @queue.vhost.name,
          },
          consumer_tag:    @tag,
          exclusive:       false,
          ack_required:    !@no_ack,
          prefetch_count:  0,
          priority:        0,
          channel_details: {
            peer_host:       @client.connection_info.remote_address.address,
            peer_port:       @client.connection_info.remote_address.port,
            connection_name: @client.name,
            user:            @client.user.name,
            number:          0_u16,
            name:            @client.name,
          },
        }
      end
    end
  end
end
