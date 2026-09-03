require "../error"
require "../schema"

module LavinMQ
  module MQTT
    # The on-disk format of `definitions.mqtt`: an append-only log of records,
    # read back by replaying it. Kept apart from `DefinitionsStore` so that
    # lavinmqctl's offline definitions generator can read the file without
    # linking the server.
    module DefinitionsFormat
      extend self

      FORMAT = IO::ByteFormat::SystemEndian

      enum Op : UInt8
        SessionAdd    = 1
        SessionDelete = 2
        Subscribe     = 3
        Unsubscribe   = 4
      end

      class InvalidRecord < LavinMQ::Error; end

      # What a replayed log adds up to. `compactable` is true if any record was
      # superseded, i.e. the file holds more than the state does.
      record Replay,
        sessions : Set(String),
        subscriptions : Hash(String, Hash(String, UInt8)),
        compactable : Bool

      # Last record wins per key and a SessionDelete drops the session's
      # subscriptions too, so replay into hashes before building anything.
      def replay(file : File) : Replay
        SchemaVersion.verify(file, :mqtt_definition)
        sessions = Set(String).new
        subscriptions = Hash(String, Hash(String, UInt8)).new
        compactable = false
        loop do
          break unless byte = file.read_byte
          op = Op.from_value?(byte) ||
               raise InvalidRecord.new("Unknown op #{byte} in #{file.path}")
          case op
          in Op::SessionAdd
            sessions << read_string(file)
          in Op::SessionDelete
            name = read_string(file)
            sessions.delete(name)
            subscriptions.delete(name)
            compactable = true
          in Op::Subscribe
            name = read_string(file)
            topic_filter = read_string(file)
            qos = file.read_byte || raise IO::EOFError.new
            subscriptions.put_if_absent(name) { Hash(String, UInt8).new }[topic_filter] = qos
          in Op::Unsubscribe
            name = read_string(file)
            topic_filter = read_string(file)
            subscriptions[name]?.try &.delete(topic_filter)
            compactable = true
          end
        rescue IO::EOFError
          break
        end
        Replay.new(sessions, subscriptions, compactable)
      end

      def session_record(op : Op, name : String) : Bytes
        io = IO::Memory.new(3 + name.bytesize)
        io.write_byte op.value
        write_string(io, name)
        io.to_slice
      end

      def subscription_record(op : Op, name : String, topic_filter : String,
                              qos : UInt8?) : Bytes
        io = IO::Memory.new(6 + name.bytesize + topic_filter.bytesize)
        io.write_byte op.value
        write_string(io, name)
        write_string(io, topic_filter)
        io.write_byte qos if qos
        io.to_slice
      end

      # u16 lengths, as in MQTT's own framing, and wider than the shortstr these
      # names used to be persisted as.
      private def write_string(io : ::IO, str : String) : Nil
        io.write_bytes(str.bytesize.to_u16, FORMAT)
        io.write(str.to_slice)
      end

      private def read_string(io : ::IO) : String
        len = UInt16.from_io(io, FORMAT)
        bytes = Bytes.new(len)
        io.read_fully(bytes)
        String.new(bytes)
      end
    end
  end
end
