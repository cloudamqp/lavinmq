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
      # Own the segment FD so reads happen on a per-reader handle rather than
      # the shared mmap. Sequential decoding lets file.pos track the logical
      # `position` after each iteration without any pread / extra seeks.
      # The FD is intentionally NOT closed in an ensure block: envelopes
      # yielded by each() carry a FileRange that points back into this FD, so
      # consumers (e.g. the spec, the HTTP introspection endpoint) need to
      # finish reading the body inside the block. The File's finalizer closes
      # the FD on GC.
      file = File.new(store.segment_path(segment), "r")
      file.read_buffering = false
      file.pos = position.to_i64
      loop do
        break if store.closed
        if file.path != store.segment_path(segment)
          file.close
          file = File.new(store.segment_path(segment), "r")
          file.read_buffering = false
          file.pos = position.to_i64
        end
        # @segments lookup races with drop_segments_while, so still take the
        # store lock for the read. The body itself stays on disk inside the
        # FileRange we return — even if drop_segments_while unlinks the
        # segment afterwards, our open FD keeps the inode alive.
        env = stream.@msg_store_lock.synchronize { store.read(segment, position, file) }
        if env
          if headers = env.message.properties.headers
            headers["x-stream-offset"] = offset
          else
            env.message.properties.headers = AMQP::Table.new({"x-stream-offset": offset})
          end
          position += env.segment_position.bytesize
          offset += 1
          # `read` left file.pos at the body start. The caller reads the
          # body out-of-band via `FileRange#read_at`, which doesn't disturb
          # file.pos, so advance past the body here to keep file.pos == the
          # next message's position for the next iteration.
          file.seek(env.message.bodysize.to_i64, IO::Seek::Current)
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
