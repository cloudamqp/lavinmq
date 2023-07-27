require "log"
require "./consumer"
require "../../segment_position"
require "../../sortable_json"
require "../../error"
require "../../queue/stream_queue_message_store"

module LavinMQ
  class Client
    class Channel
      class StreamConsumer < LavinMQ::Client::Channel::Consumer
        include StreamQueue::StreamPosition

        def initialize(@channel : Client::Channel, @queue : StreamQueue, @frame : AMQP::Frame::Basic::Consume)
          @offset = stream_offset(@frame)
          stream_queue.find_offset(self)
          super
        end

        private def deliver_loop
          i = 0
          loop do
            wait_for_capacity
            loop do
              raise ClosedError.new if @closed
              next if wait_for_global_capacity
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
          if stream_queue.last_offset <= @offset
            @log.debug { "Waiting for queue not to be empty" }
            select
            when stream_queue.new_messages.receive
              @log.debug { "Queue is not empty" }
            when @notify_closed.receive
            end
            return true
          end
        end

        private def stream_queue : StreamQueue
          @queue.as(StreamQueue)
        end

        def reject(sp, requeue : Bool)
          super
          @requeued.push(sp) if requeue
        end

        private def stream_offset(frame) : Int64
          case offset = frame.arguments["x-stream-offset"]?
          when Nil, "first"   then 0i64
          when "next", "last" then stream_queue.last_offset
          when Int            then offset.to_i64
            # TODO: support timestamps
          else
            raise Error::PreconditionFailed.new("x-stream-offset must be an integer, first, next or last")
          end
        end
      end
    end
  end
end
