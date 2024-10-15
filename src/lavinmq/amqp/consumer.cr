require "log"
require "../client/channel/consumer"
require "../logger"

module LavinMQ
  module AMQP
    class Consumer < LavinMQ::Client::Channel::Consumer
      include SortableJSON
      Log = LavinMQ::Log.for "amqp.consumer"
      getter tag : String
      getter priority : Int32
      getter? exclusive : Bool
      getter? no_ack : Bool
      getter channel, queue
      getter prefetch_count = 0u16
      getter unacked = 0_u32
      getter? closed = false
      @flow : Bool
      @metadata : ::Log::Metadata

      def initialize(@channel : AMQP::Channel, @queue : Queue, frame : AMQP::Frame::Basic::Consume)
        @tag = frame.consumer_tag
        @no_ack = frame.no_ack
        @exclusive = frame.exclusive
        @priority = consumer_priority(frame) # Must be before ConsumeOk, can close channel
        @prefetch_count = @channel.prefetch_count
        @flow = @channel.flow?
        @metadata = @channel.@metadata.extend({consumer: @tag})
        @log = Logger.new(Log, @metadata)
        spawn deliver_loop, name: "Consumer deliver loop"
      end

      def close
        @closed = true
        @queue.rm_consumer(self)
        @notify_closed.close
        @has_capacity.close
        @flow_change.close
      end

      @notify_closed = ::Channel(Nil).new
      @flow_change = ::Channel(Bool).new

      def flow(active : Bool)
        @flow = active
        @flow_change.try_send? active
      end

      def prefetch_count=(prefetch_count : UInt16)
        @prefetch_count = prefetch_count
        notify_has_capacity(@prefetch_count > @unacked)
      end

      private def deliver_loop
        wait_for_single_active_consumer
        queue = @queue
        i = 0
        loop do
          wait_for_capacity
          loop do
            raise ClosedError.new if @closed
            next if wait_for_global_capacity
            next if wait_for_priority_consumers
            next if wait_for_queue_ready
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
          queue.consume_get(self) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
          end
          Fiber.yield if (i &+= 1) % 32768 == 0
        end
      rescue ex : ClosedError | Queue::ClosedError | AMQP::Channel::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      end

      private def wait_for_global_capacity
        ch = @channel
        return if ch.has_capacity?
        @log.debug { "Waiting for global prefetch capacity" }
        select
        when ch.has_capacity.receive
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
          if @queue.consumers.any? { |c| c.priority > @priority && c.accepts? }
            @log.debug { "Waiting for higher priority consumers to not have capacity" }
            begin
              ::Channel.receive_first(@queue.consumers.map(&.has_capacity))
            rescue ::Channel::ClosedError
            end
            return true
          end
        end
      end

      private def wait_for_queue_ready
        if @queue.empty?
          @log.debug { "Waiting for queue not to be empty" }
          select
          when is_empty = @queue.empty_change.receive
            @log.debug { "Queue is #{is_empty ? "" : "not"} empty" }
          when @notify_closed.receive
          end
          return true
        end
      end

      private def wait_for_paused_queue
        if @queue.paused?
          @log.debug { "Waiting for queue not to be paused" }
          select
          when is_paused = @queue.paused_change.receive
            @log.debug { "Queue is #{is_paused ? "" : "not"} paused" }
          when @notify_closed.receive
          end
          return true
        end
      end

      private def wait_for_flow
        unless @flow
          @log.debug { "Waiting for flow" }
          is_flow = @flow_change.receive
          @log.debug { "Channel flow=#{is_flow}" }
          return true
        end
      end

      # blocks until the consumer can accept more messages
      private def wait_for_capacity : Nil
        if @prefetch_count > 0
          until @unacked < @prefetch_count
            @log.debug { "Waiting for prefetch capacity" }
            @has_capacity.receive
          end
        end
      end

      def accepts? : Bool
        return false unless @flow
        return false if @prefetch_count > 0 && @unacked >= @prefetch_count
        return false if @channel.global_prefetch_count > 0 && @channel.consumers.sum(&.unacked) >= @channel.global_prefetch_count
        true
      end

      getter has_capacity = ::Channel(Bool).new

      private def notify_has_capacity(value)
        while @has_capacity.try_send? value
        end
      end

      def deliver(msg, sp, redelivered = false, recover = false)
        unless @no_ack || recover
          @unacked += 1
          notify_has_capacity(false) if @unacked == @prefetch_count
        end
        delivery_tag = @channel.next_delivery_tag(@queue, sp, @no_ack, self)
        deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
          delivery_tag,
          redelivered,
          msg.exchange_name, msg.routing_key)
        @channel.deliver(deliver, msg, redelivered)
      end

      def ack(sp)
        was_full = @unacked == @prefetch_count
        @unacked -= 1
        notify_has_capacity(true) if was_full
      end

      def reject(sp, requeue = false)
        was_full = @unacked == @prefetch_count
        @unacked -= 1
        notify_has_capacity(true) if was_full
      end

      def cancel
        @channel.send AMQP::Frame::Basic::Cancel.new(@channel.id, @tag, no_wait: true)
        @channel.consumers.delete self
        close
      end

      private def consumer_priority(frame) : Int32
        case prio = frame.arguments["x-priority"]
        when Int then prio.to_i32
        else          raise LavinMQ::Error::PreconditionFailed.new("x-priority must be an integer")
        end
      rescue KeyError
        0
      rescue OverflowError
        raise LavinMQ::Error::PreconditionFailed.new("x-priority out of bounds, must fit a 32-bit integer")
      end

      def unacked_messages
        @channel.unacked
      end

      def channel_name
        @channel.name
      end

      def details_tuple
        channel_details = @channel.details_tuple
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
            peer_host:       channel_details[:connection_details][:peer_host]?,
            peer_port:       channel_details[:connection_details][:peer_port]?,
            connection_name: channel_details[:connection_details][:name],
            user:            channel_details[:user],
            number:          channel_details[:number],
            name:            channel_details[:name],
          },
        }
      end

      class ClosedError < Error; end
    end
  end
end
