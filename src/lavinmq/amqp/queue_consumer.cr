require "log"
require "../client/channel/consumer"
require "../logger"
require "../bool_channel"
require "../config"
require "../error"

module LavinMQ
  module AMQP
    # A consumer of a queue, independent of how messages reach it: the
    # deliver loop and every condition it waits for before taking the next
    # message (prefetch, single active consumer, priority consumers, a paused
    # queue, flow), and the unacked count.
    #
    # AMQP::Consumer delivers to an AMQP channel, Endpoint::LocalConsumer to
    # an in-process shovel or federation link. Subclasses deliver messages
    # (#deliver) and provide the channel-wide parts: a global prefetch, if
    # any, flushing, and counting deliveries.
    abstract class QueueConsumer < LavinMQ::Client::Channel::Consumer
      include SortableJSON
      getter tag : String
      getter priority : Int32
      getter? exclusive : Bool
      getter? no_ack : Bool
      getter queue
      getter prefetch_count : UInt16
      getter? closed = false
      getter has_capacity : BoolChannel
      @flow : Bool
      @unacked = Atomic(UInt32).new(0_u32)
      @deliver_loop_running = Atomic(Bool).new(false)
      @notify_closed = ::Channel(Nil).new
      @log : Logger

      def initialize(@queue : Queue, @tag : String, @no_ack : Bool, @exclusive : Bool,
                     @priority : Int32, @prefetch_count : UInt16, @flow : Bool, @log : Logger)
        @flow_change = BoolChannel.new(@flow)
        @has_capacity = BoolChannel.new(true)
      end

      # Delivers a message the deliver loop got from the queue
      abstract def deliver(msg, sp, redelivered = false, recover = false)

      # Wait for capacity in the channel (AMQP's global prefetch), if any
      private abstract def global_capacity_signal : ::Channel(Nil)?

      # Whether the channel's own prefetch is full (AMQP's global prefetch)
      private abstract def global_prefetch_full? : Bool

      # Called with each message delivered, after #deliver
      private abstract def count_delivery(redelivered : Bool) : Nil

      # Called before the loop waits
      abstract def flush

      def close
        return if @closed
        @closed = true
        @notify_closed.close
        @has_capacity.close
        @flow_change.close
      end

      def ensure_deliver_loop
        return if @closed
        return if @deliver_loop_running.swap(true, :acquire_release)
        fiber_name = "Consumer vhost=#{@queue.vhost.name} queue=#{@queue.name}"
        @queue.deliver_loop_wg.spawn(name: fiber_name) { deliver_loop }
      end

      def flow(active : Bool)
        @flow = active
        @flow_change.set(active)
      end

      def prefetch_count=(prefetch_count : UInt16)
        @prefetch_count = prefetch_count
        @has_capacity.set(@prefetch_count > @unacked.get(:relaxed))
      end

      def unacked
        @unacked.get(:relaxed)
      end

      # A server generated consumer tag
      def self.generate_tag : String
        "amq.ctag-#{Random::Secure.urlsafe_base64(24)}"
      end

      # The channel (or session) the consumer belongs to, for #details_tuple
      private abstract def channel_details

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
          channel_details: channel_details,
        }
      end

      # A deliver loop that has nothing to deliver exits after
      # deliver_loop_idle_timeout, and is restarted by #ensure_deliver_loop
      # when the queue gets messages. One that runs on its caller's fiber
      # can't exit, and waits for messages without a timeout instead.
      private def exit_when_idle? : Bool
        true
      end

      private def deliver_loop
        wait_for_single_active_consumer
        queue = @queue
        delivered_bytes = 0_i32
        loop do
          wait_for_capacity
          loop do
            raise ClosedError.new if @closed
            next if wait_for_global_capacity
            next if wait_for_priority_consumers
            case wait_for_queue_ready
            when :timeout
              @log.debug { "deliver loop idle, exiting fiber" }
              @deliver_loop_running.set(false, :release)
              ensure_deliver_loop unless @queue.empty?
              return
            when :ready
              next
            end
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
          queue.consume_get(@no_ack) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
            delivered_bytes &+= env.segment_position.bytesize
            count_delivery(env.redelivered)
          end
          if delivered_bytes > Config.instance.yield_each_delivered_bytes
            delivered_bytes = 0
            Fiber.yield
          end
        end
      rescue ex : ClosedError | Queue::ClosedError | Channel::ClosedError | ::Channel::ClosedError | IO::Error
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
        @deliver_loop_running.set(false, :release)
      rescue ex
        @log.debug { "deliver loop exiting unexpectedly: #{ex.inspect}" }
        @deliver_loop_running.set(false, :release)
        raise ex
      end

      private def wait_for_global_capacity
        signal = global_capacity_signal || return
        @log.debug { "Waiting for global prefetch capacity" }
        flush
        select
        when signal.receive
        when @notify_closed.receive
        end
        true
      end

      private def wait_for_single_active_consumer
        case @queue.single_active_consumer
        when self
          @log.debug { "This consumer is the single active consumer" }
        when nil
          @log.debug { "The queue isn't a single active consumer queue" }
        else
          @log.debug { "Waiting for this consumer to become the single active consumer" }
          flush
          loop do
            select
            when sca = @queue.single_active_consumer_change.receive
              if sca == self
                break
              else
                @log.debug { "New single active consumer, but not me" }
              end
            when @notify_closed.receive
              break
            end
          end
          true
        end
      end

      private def wait_for_priority_consumers
        # single active consumer queues can't have priority consumers
        if @queue.has_priority_consumers? && @queue.single_active_consumer.nil?
          @log.debug { "Waiting for higher priority consumers to not have capacity" }
          flush
          higher_prio_consumers = @queue.consumers.select { |c| c.priority > @priority }
          return false unless higher_prio_consumers.any? &.accepts?
          loop do
            # FIXME: doesnt take into account that new higher prio consumer might connect
            ::Channel.receive_first(higher_prio_consumers.map(&.has_capacity.when_false))
            break
          rescue ::Channel::ClosedError
            higher_prio_consumers = @queue.consumers.select { |c| c.priority > @priority }
            break if higher_prio_consumers.empty?
            next
          end
          true
        end
      end

      private def wait_for_queue_ready
        return unless @queue.empty?
        @log.debug { "Waiting for queue not to be empty" }
        flush
        unless exit_when_idle?
          select
          when @queue.empty.when_false.receive
            return :ready
          when @notify_closed.receive
            raise ClosedError.new
          end
        end
        select
        when @queue.empty.when_false.receive
          :ready
        when @notify_closed.receive
          raise ClosedError.new
        when timeout Config.instance.deliver_loop_idle_timeout
          @log.debug { "Deliver loop idle timeout" }
          :timeout
        end
      end

      private def wait_for_paused_queue
        if @queue.state.paused?
          @log.debug { "Waiting for queue not to be paused" }
          flush
          select
          when @queue.paused.when_false.receive
            @log.debug { "Queue is not paused" }
          when @notify_closed.receive
          end
          true
        end
      end

      private def wait_for_flow
        unless @flow
          @log.debug { "Waiting for flow" }
          flush
          @flow_change.when_true.receive
          @log.debug { "Channel flow=true" }
          true
        end
      end

      # blocks until the consumer can accept more messages
      private def wait_for_capacity : Nil
        if @prefetch_count > 0
          until @unacked.get(:relaxed) < @prefetch_count
            @log.debug { "Waiting for prefetch capacity" }
            flush
            @has_capacity.when_true.receive
          end
        end
      end

      def accepts? : Bool
        return false unless @flow
        return false if @prefetch_count > 0 && @unacked.get(:relaxed) >= @prefetch_count
        return false if global_prefetch_full?
        true
      end

      # The signal for the first condition of #accepts? that fails, nil when
      # nothing signals it (see Client::Channel::Consumer#accepts_signal)
      def accepts_signal : ::Channel(Nil)?
        return @flow_change.when_true unless @flow
        if @prefetch_count > 0 && @unacked.get(:relaxed) >= @prefetch_count
          return @has_capacity.when_true
        end
        global_capacity_signal
      end

      # Counts a delivery that must be acked, before it's delivered
      protected def track_unacked : Nil
        unacked = @unacked.add(1, :relaxed)
        @has_capacity.set(false) if (unacked + 1) == @prefetch_count
      end

      def ack(sp)
        unacked = @unacked.sub(1, :relaxed)
        @has_capacity.set(true) if unacked == @prefetch_count
      end

      def reject(sp, requeue = false)
        unacked = @unacked.sub(1, :relaxed)
        @has_capacity.set(true) if unacked == @prefetch_count
      end

      class ClosedError < Error; end
    end
  end
end
