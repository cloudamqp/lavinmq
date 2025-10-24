require "../amqp/stream/stream"
require "../amqp/stream/stream_message_store"

module LavinMQ
  module JobQueue
    # Helper module for reading messages from stream queues
    module StreamReader
      # Iterate through all messages in a stream from a given offset
      def self.each_message(queue : AMQP::Stream, from_offset : Int64 = 0, &block : BytesMessage, Int64 ->)
        store = queue.@msg_store.as(AMQP::StreamMessageStore)

        # Return early if no segments (empty stream)
        return if store.@segments.empty?

        # Find starting position
        begin
          current_offset, segment_id, pos = store.find_offset(from_offset > 0 ? from_offset : "first")
        rescue
          # Stream is empty or offset not found
          return
        end

        # Iterate through segments
        store.@segments.each do |seg_id, mfile|
          next if seg_id < segment_id
          start_pos = (seg_id == segment_id) ? pos : 4_u32

          begin
            mfile.pos = start_pos
            loop do
              msg = BytesMessage.from_bytes(mfile.to_slice + mfile.pos)

              # Extract offset from headers
              msg_offset = if headers = msg.properties.headers
                             headers["x-stream-offset"]?.as?(Int64) || current_offset
                           else
                             current_offset
                           end

              # Skip if we haven't reached the starting offset yet
              if msg_offset >= current_offset
                yield msg, msg_offset
              end

              # Move to next message
              BytesMessage.skip(mfile)
            end
          rescue IO::EOFError
            # End of segment, continue to next
          rescue IndexError
            # Malformed message or end of data
            break
          end
        end
      end

      # Count messages in a stream
      def self.count_messages(queue : AMQP::Stream) : Int64
        count = 0_i64
        each_message(queue) { |_, _| count += 1 }
        count
      end

      # Find a message by predicate
      def self.find_message(queue : AMQP::Stream, &block : BytesMessage, Int64 -> Bool) : Tuple(BytesMessage, Int64)?
        each_message(queue) do |msg, offset|
          return {msg, offset} if yield(msg, offset)
        end
        nil
      end
    end
  end
end
