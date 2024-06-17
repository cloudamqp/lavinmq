require "./consumer"
require "../../segment_position"

module LavinMQ
  class Client
    class Channel
      class StreamConsumer < LavinMQ::Client::Channel::Consumer
        property offset : Int64
        property segment : UInt32
        property pos : UInt32
        getter requeued = Deque(SegmentPosition).new
        @track_offset = false

        def initialize(@channel : Client::Channel, @queue : StreamQueue, frame : AMQP::Frame::Basic::Consume)
          @tag = frame.consumer_tag
          validate_preconditions(frame)
          offset = frame.arguments["x-stream-offset"]?
          @offset, @segment, @pos = stream_queue.find_offset(offset, @tag, @track_offset)
          super
        end

        private def validate_preconditions(frame)
          if frame.exclusive
            raise Error::PreconditionFailed.new("Stream consumers must not be exclusive")
          end
          if frame.no_ack
            raise Error::PreconditionFailed.new("Stream consumers must acknowledge messages")
          end
          if @channel.prefetch_count.zero?
            raise Error::PreconditionFailed.new("Stream consumers must have a prefetch limit")
          end
          unless @channel.global_prefetch_count.zero?
            raise Error::PreconditionFailed.new("Stream consumers does not support global prefetch limit")
          end
          if frame.arguments.has_key? "x-priority"
            raise Error::PreconditionFailed.new("x-priority not supported on stream queues")
          end
          case frame.arguments["x-stream-offset"]?
          when Nil
            @track_offset = true unless @tag.starts_with?("amq.ctag-")
          when Int, Time, "first", "next", "last"
            @track_offset = true if frame.arguments["x-stream-automatic-offset-tracking"]?
          else raise Error::PreconditionFailed.new("x-stream-offset must be an integer, a timestamp, 'first', 'next' or 'last'")
          end
        end

        private def deliver_loop
          i = 0
          loop do
            wait_for_capacity
            loop do
              raise ClosedError.new if @closed
              next if wait_for_queue_ready
              next if wait_for_paused_queue
              next if wait_for_flow
              break
            end
            {% unless flag?(:release) %}
              @log.debug { "Getting a new message" }
            {% end %}
            stream_queue.consume_get(self) do |env|
              deliver(env.message, env.segment_position, env.redelivered)
            end
            Fiber.yield if (i &+= 1) % 32768 == 0
          end
        rescue ex : ClosedError | Queue::ClosedError | Client::Channel::ClosedError | ::Channel::ClosedError
          @log.debug { "deliver loop exiting: #{ex.inspect}" }
        end

        private def wait_for_queue_ready
          if @offset > stream_queue.last_offset && @requeued.empty?
            @log.debug { "Waiting for queue not to be empty" }
            select
            when stream_queue.new_messages.receive
              @log.debug { "Queue is not empty" }
            when @has_requeued.receive
              @log.debug { "Got a requeued message" }
            when @notify_closed.receive
            end
            return true
          end
        end

        @has_requeued = ::Channel(Nil).new

        private def stream_queue : StreamQueue
          @queue.as(StreamQueue)
        end

        def ack(sp)
          stream_queue.update_consumer_offset(@tag, @offset) if @track_offset
          super
        end

        def reject(sp, requeue : Bool)
          super
          if requeue
            @requeued.push(sp)
            @has_requeued.try_send? nil if @requeued.size == 1
          end
        end
      end
    end
  end
end
