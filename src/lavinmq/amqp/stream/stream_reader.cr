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
        if leased = stream.read_leased(segment, position)
          env, mfile = leased
          begin
            if headers = env.message.properties.headers
              headers["x-stream-offset"] = offset
            else
              env.message.properties.headers = AMQP::Table.new({"x-stream-offset": offset})
            end
            position += env.segment_position.bytesize
            offset += 1
            yield env
          ensure
            mfile.release_lease
          end
          stream.@deliver_get_count.add(1, :relaxed)
        else
          # try read from new segment, the current one may also have been
          # dropped by retention, so take the offset from the next segment
          s, offset = stream.next_segment_offset(segment) || break
          stream.unmap_if_unused(segment)
          position = 4u32
          segment = s
        end
      end
    ensure
      @stream.unmap_if_unused(segment) if segment
    end
  end
end
