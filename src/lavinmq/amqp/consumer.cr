require "log"
require "wait_group"
require "../amqp"
require "../client/channel/consumer"
require "../logger"
require "../bool_channel"

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
      getter prefetch_count : UInt16
      getter? closed = false
      @flow : Bool
      @metadata : ::Log::Metadata
      @unacked = Atomic(UInt32).new(0_u32)
      getter has_capacity = BoolChannel.new(true)

      def initialize(@channel : AMQP::Channel, @queue : Queue, frame : AMQP::Frame::Basic::Consume)
        @tag = frame.consumer_tag
        @no_ack = frame.no_ack
        @exclusive = frame.exclusive
        @priority = consumer_priority(frame) # Must be before ConsumeOk, can close channel
        @prefetch_count = @channel.prefetch_count
        @flow = @channel.flow?
        @flow_change = BoolChannel.new(@flow)
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

      def flow(active : Bool)
        @flow = active
        @flow_change.set(active)
      end

      def prefetch_count=(prefetch_count : UInt16)
        @prefetch_count = prefetch_count
        @has_capacity.set(@prefetch_count > @unacked.get)
      end

      def unacked
        @unacked.get
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
            next if wait_for_queue_ready
            next if wait_for_paused_queue
            next if wait_for_flow
            break
          end
          {% unless flag?(:release) %}
            @log.debug { "Getting a new message" }
          {% end %}
          queue.consume_get(@no_ack) do |env|
            deliver(env.message, env.segment_position, env.redelivered)
            delivered_bytes &+= env.message.bytesize
            @channel.increment_deliver_count(env.redelivered, @no_ack)
          end
          if delivered_bytes > Config.instance.yield_each_delivered_bytes
            delivered_bytes = 0
            Fiber.yield
          end
        end
      rescue ex : ClosedError | Queue::ClosedError | AMQP::Channel::ClosedError | ::Channel::ClosedError
        @log.debug { "deliver loop exiting: #{ex.inspect}" }
      end

      private def wait_for_global_capacity
        ch = @channel
        return if ch.has_capacity?
        @log.debug { "Waiting for global prefetch capacity" }
        select
        when ch.has_capacity.when_true.receive
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
          @log.debug { "Waiting for higher priority consumers to not have capacity" }
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
          return true
        end
      end

      private def wait_for_queue_ready
        if @queue.empty?
          @log.debug { "Waiting for queue not to be empty" }
          select
          when @queue.empty.when_false.receive
          when @notify_closed.receive
          end
          return true
        end
      end

      private def wait_for_paused_queue
        if @queue.state.paused?
          @log.debug { "Waiting for queue not to be paused" }
          select
          when @queue.paused.when_false.receive
            @log.debug { "Queue is not paused" }
          when @notify_closed.receive
          end
          return true
        end
      end

      private def wait_for_flow
        unless @flow
          @log.debug { "Waiting for flow" }
          @flow_change.when_true.receive
          @log.debug { "Channel flow=true" }
          return true
        end
      end

      # blocks until the consumer can accept more messages
      private def wait_for_capacity : Nil
        if @prefetch_count > 0
          until @unacked.get < @prefetch_count
            @log.debug { "Waiting for prefetch capacity" }
            @has_capacity.when_true.receive
          end
        end
      end

      def accepts? : Bool
        return false unless @flow
        return false if @prefetch_count > 0 && @unacked.get >= @prefetch_count
        return false if @channel.global_prefetch_count > 0 && @channel.consumers.sum(&.unacked) >= @channel.global_prefetch_count
        true
      end

      def deliver(msg, sp, redelivered = false, recover = false)
        unless @no_ack || recover
          unacked = @unacked.add(1)
          @has_capacity.set(false) if (unacked + 1) == @prefetch_count
        end
        delivery_tag = @channel.next_delivery_tag(@queue, sp, @no_ack, self)
        deliver = AMQP::Frame::Basic::Deliver.new(@channel.id, @tag,
          delivery_tag,
          redelivered,
          msg.exchange_name, msg.routing_key)
        @channel.deliver(deliver, msg, redelivered)
      end

      def ack(sp)
        unacked = @unacked.sub(1)
        @has_capacity.set(true) if unacked == @prefetch_count
      end

      def reject(sp, requeue = false)
        unacked = @unacked.sub(1)
        @has_capacity.set(true) if unacked == @prefetch_count
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
            peer_host:       channel_details[:connection_details][:peer_host],
            peer_port:       channel_details[:connection_details][:peer_port],
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
