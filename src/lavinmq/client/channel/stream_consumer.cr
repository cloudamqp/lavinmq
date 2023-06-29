require "log"
require "../../sortable_json"
require "../../error"

module LavinMQ
  class Client
    class Channel
      class StreamConsumer < LavinMQ::Client::Channel::Consumer
        # @offset : Int64
        @segment = 1_u32
        @offset = 0_i64
        @pos = 4_u32
        @requeued = Deque(SegmentPosition).new
        getter requeued
        getter empty_change = ::Channel(Bool).new
        property segment, pos, offset

        def initialize(@channel : Client::Channel, @queue : StreamQueue, @frame : AMQP::Frame::Basic::Consume)
          @offset = stream_offset(@frame)
          super
        end

        private def deliver_loop
          puts "deliver_loop"
          queue = @queue
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
            queue.consume_get(self) do |env|
              deliver(env.message, env.segment_position, env.redelivered)
            end
            Fiber.yield if (i &+= 1) % 32768 == 0
          end
        rescue ex : ClosedError | Queue::ClosedError | Client::Channel::ClosedError | ::Channel::ClosedError
          @log.debug { "deliver loop exiting: #{ex.inspect}" }
        end

        private def wait_for_queue_ready
          if queue.empty?
            @log.debug { "Waiting for queue not to be empty" }
            select
            when is_empty = empty_change.receive
              @log.debug { "Queue is #{is_empty ? "" : "not"} empty" }
            when @notify_closed.receive
            end
            return true
          end
        end

        # def update_segment(segment, pos)
        #  @segment = segment
        #  @pos = pos
        # end

        # def update_offset(offset)
        #  @offset = offset
        # end

        def reject(unack, requeue)
          @requeued.push(unack.sp) if requeue
        end

        def requeue(sp)
          @requeued.push sp
        end

        private def stream_offset(frame) : Int64?
          offset = 0_i64
          if offset_arg = frame.arguments["x-stream-offset"]?
            case offset_arg     # TODO: support timestamps
            when "first"        # offset = 0
            when "next", "last" # last should be last "chunk", but we don't support that yet
              offset = queue.as(StreamQueue).last_offset
            when offset_int = offset_arg.as?(Int)
              offset = (offset_int || 0).to_i64 # FIX ME!
            else
              raise Error::PreconditionFailed.new("x-stream-offset must be an integer, first, next or last")
            end
          else
            offset = queue.as(StreamQueue).last_offset
          end
          offset
        end
      end
    end
  end
end
