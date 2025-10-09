require "../stream/stream_message_store"
require "../stream/stream"

module LavinMQ::AMQP
  class StreamReader
    Log = LavinMQ::Log.for "stream_reader"

    def initialize(@store : StreamMessageStore, @start_offset : String | Int64 | Time)
    end

    def each(&)
      store = @store
      _, segment, position = store.find_offset(@start_offset)
      loop do
        break if store.closed
        env = store.read(segment, position)
        if env
          position += env.segment_position.bytesize
        else
          # try read from new segment
          s = store.next_segment_id(segment) || break
          position = 4u32
          segment = s
          next
        end
        yield env
      end
    end
  end
end
