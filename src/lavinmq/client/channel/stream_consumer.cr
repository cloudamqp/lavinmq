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
          if empty?
            @log.debug { "Waiting for queue not to be empty" }
            select
            when stream_queue.new_messages.receive
              @log.debug { "Queue is not empty" }
            when @notify_closed.receive
            end
            return true
          end
        end

        def empty?
          stream_queue.last_offset <= @offset
        end

        def stream_queue : StreamQueue
          @queue.as(StreamQueue)
        end

        def reject(sp, requeue : Bool)
          super
          @requeued.push(sp) if requeue
        end

        private def stream_offset(frame) : Int64?
          offset = 0_i64
          if offset_arg = frame.arguments["x-stream-offset"]?
            case offset_arg     # TODO: support timestamps
            when "first"        # offset = 0
            when "next", "last" # last should be last "chunk", but we don't support that yet
              offset = stream_queue.last_offset
            when offset_int = offset_arg.as?(Int)
              offset = (offset_int || 0).to_i64 # FIX ME!
            else
              raise Error::PreconditionFailed.new("x-stream-offset must be an integer, first, next or last")
            end
          else
            offset = stream_queue.last_offset
          end
          offset
        end
      end
    end
  end
end
