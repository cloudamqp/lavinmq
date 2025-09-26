require "../stream/stream_message_store"
require "../stream/stream"

module LavinMQ::AMQP
  class StreamReader
    Log = LavinMQ::Log.for "stream_reader"

    def initialize(@store : StreamStore, @start_offset : String | Int64 | Time)
    end

    def each(&)
      store = @store
      msg_offset, segment, position = store.find_offset(@start_offset)
      loop do
        env = store.read(segment, position)
        if env
          position += env.segment_position.bytesize
        else
          # try read from new segment
          _, s, p = store.find_offset(msg_offset)
          env = store.read(s, p)
          break unless env # No more messages to read, i.e we reached last segment
          segment = s
          position = p
        end
        msg_offset += 1
        yield env
      rescue e : StreamMessageStore::Error
        Log.error { "store error: #{e}" }
        return
      end
    end
  end
end
