require "../stream/stream_message_store"
require "../stream/stream"

module LavinMQ::AMQP
  class StreamReader
    Log = LavinMQ::Log.for "stream_reader"

    def initialize(@stream : Stream, @start_offset : String | Int64 | Time)
    end

    def each(&)
      stream = @stream
      store = stream.stream_msg_store
      offset, segment, position = store.find_offset(@start_offset)
      loop do
        break if store.closed
        env = store.read(segment, position)
        if env
          if headers = env.message.properties.headers
            headers["x-stream-offset"] = offset
          else
            env.message.properties.headers = AMQP::Table.new({"x-stream-offset": offset})
          end
          position += env.segment_position.bytesize
          offset += 1
        else
          # try read from new segment
          s = store.next_segment_id(segment) || break
          position = 4u32
          segment = s
          next
        end
        yield env
        stream.@deliver_get_count.add(1, :relaxed)
      end
    end
  end
end
