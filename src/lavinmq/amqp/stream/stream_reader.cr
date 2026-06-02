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
      offset, segment, position = stream.find_offset(@start_offset)
      loop do
        break if store.closed
        delivered = stream.read(segment, position) do |env|
          if headers = env.message.properties.headers
            headers["x-stream-offset"] = offset
          else
            env.message.properties.headers = AMQP::Table.new({"x-stream-offset": offset})
          end
          position += env.segment_position.bytesize
          offset += 1
          yield env
          stream.@deliver_get_count.add(1, :relaxed)
        end
        unless delivered
          # try read from new segment
          s = store.next_segment_id(segment) || break
          position = 4u32
          segment = s
          next
        end
      end
    end
  end
end
