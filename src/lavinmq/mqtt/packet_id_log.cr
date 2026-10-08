require "../filesystem"
require "../mfile"
require "../segment_position"
require "../persister"
require "../clustering/replicator"
require "../error"

module LavinMQ
  module MQTT
    # Backs the QoS 2 packet ids a durable session holds, in both directions,
    # so they survive a restart and a failover. Append-only, compacted from
    # its own live view. States are named after the packet that causes them.
    class PacketIdLog
      Log = LavinMQ::Log.for("packetidlog")

      # Not an IO::Error, so a disk failure is not mistaken for a socket one
      class Error < LavinMQ::Error; end

      SCHEMA_VERSION    = 1u32
      HEADER_SIZE       =    4
      RECORD_SIZE       =    3
      PUBLISH_SENT_SIZE =   11
      # Capacity of a new file, and the floor of a compacted one. A page
      # multiple, and room for a few thousand records between compactions.
      MIN_CAPACITY = 16 * 1024

      enum State : UInt8
        PublishReceived = 1 # inbound, held until PUBREL
        PubrelReceived  = 2
        # Written before the send: the subscriber may hold the id from here
        PublishSent     = 3
        PubcompReceived = 4
      end

      getter awaiting_pubrel = Set(UInt16).new
      getter publish_sent = Hash(UInt16, SegmentPosition).new
      @lock = Mutex.new
      # Mapped on the first append: most durable sessions never use QoS 2,
      # and should cost neither the fsyncs of creating the file nor a mapping.
      @mfile : MFile? = nil
      @closed = false

      def initialize(@path : String, @replicator : Clustering::Replicator?, @persister : Persister?)
        load if File.exists?(@path)
      end

      def publish_received(id : UInt16) : Nil
        append(State::PublishReceived, id) { @awaiting_pubrel << id }
      end

      def pubrel_received(id : UInt16) : Nil
        append(State::PubrelReceived, id) { @awaiting_pubrel.delete(id) }
      end

      def publish_sent(id : UInt16, sp : SegmentPosition) : Nil
        append(State::PublishSent, id, sp) { @publish_sent[id] = sp }
      end

      def pubcomp_received(id : UInt16) : Nil
        append(State::PubcompReceived, id) { @publish_sent.delete(id) }
      end

      # Compaction runs inside append, under the lock, and never calls close,
      # so taking the lock here cannot deadlock. MFile#close truncates the
      # file to its logical size.
      def close : Nil
        @lock.synchronize do
          @closed = true
          @mfile.try &.close
        end
      end

      def delete : Nil
        @lock.synchronize do
          @closed = true
          if mfile = @mfile
            mfile.delete(raise_on_missing: false)
            mfile.close
          end
          File.delete?(@path)
        end
        @replicator.try &.delete_file(@path)
      end

      # The live view changes under the lock, after the write, so memory and
      # disk cannot disagree and compaction never iterates a changing collection.
      private def append(state : State, id : UInt16, sp : SegmentPosition? = nil, & : ->) : Nil
        @lock.synchronize do
          buf = uninitialized UInt8[PUBLISH_SENT_SIZE]
          bytes = encode(buf.to_slice, state, id, sp)
          raise IO::Error.new("closed") if @closed
          mfile = @mfile || compact
          begin
            mfile.write bytes
          rescue IO::EOFError
            mfile = compact
            mfile.write bytes
          end
          yield
          @replicator.try &.append(mfile.path, mfile.size - bytes.size, bytes.size)
          @persister.try &.mark_dirty(mfile)
        end
      rescue ex : IO::Error | RuntimeError
        raise Error.new("#{@path}: #{ex.message}", cause: ex)
      end

      private def encode(buf : Bytes, state : State, id : UInt16, sp : SegmentPosition?) : Bytes
        buf[0] = state.value
        IO::ByteFormat::LittleEndian.encode(id, buf[1, 2])
        return buf[0, RECORD_SIZE] unless sp
        IO::ByteFormat::LittleEndian.encode(sp.segment, buf[3, 4])
        IO::ByteFormat::LittleEndian.encode(sp.position, buf[7, 4])
        buf[0, PUBLISH_SENT_SIZE]
      end

      # Header plus the live set (both directions) into a new file, made
      # durable under a tmp name before it replaces the old one, so a crash
      # leaves one or the other. Also creates the file, and drops whatever a
      # torn tail left behind. Fsyncs on the scheduler thread, a stall only
      # QoS 2 pays.
      private def compact : MFile
        live = HEADER_SIZE + @awaiting_pubrel.size * RECORD_SIZE + @publish_sent.size * PUBLISH_SENT_SIZE
        tmp_path = "#{@path}.tmp"
        File.delete?(tmp_path)
        mfile = map(tmp_path, Math.max(MIN_CAPACITY, 2 * live + PUBLISH_SENT_SIZE))
        begin
          write_live(mfile)
          FileSystem.durable_rename(mfile, @path)
        rescue ex
          mfile.close(truncate_to_size: false)
          File.delete?(tmp_path)
          raise ex
        end
        # Also registers it, and the follower appends after this content
        @replicator.try &.replace_file(mfile)
        # Not truncated: the path is the new file's now
        @mfile.try &.close(truncate_to_size: false)
        @mfile = mfile
      end

      private def write_live(mfile : MFile) : Nil
        buf = uninitialized UInt8[PUBLISH_SENT_SIZE]
        IO::ByteFormat::LittleEndian.encode(SCHEMA_VERSION, buf.to_slice[0, HEADER_SIZE])
        mfile.write buf.to_slice[0, HEADER_SIZE]
        @awaiting_pubrel.each { |id| mfile.write encode(buf.to_slice, State::PublishReceived, id, nil) }
        @publish_sent.each { |id, sp| mfile.write encode(buf.to_slice, State::PublishSent, id, sp) }
      end

      # Page sized folios, so a sync doesn't rewrite a large folio for each
      # 3 byte append (see MessageStore#open_ack_file)
      private def map(path : String, capacity : Int) : MFile
        mfile = MFile.new(path, capacity)
        mfile.advise(MFile::Advice::Random)
        mfile
      end

      # A short or invalid record ends the log: power loss leaves zeros at the
      # tail, and nothing was acted on before its record was durable. After a
      # crash the file is capacity-sized, so the scan stops at the zero tail.
      private def load : Nil
        mfile = map(@path, MIN_CAPACITY)
        @mfile = mfile
        valid = scan(mfile.to_slice)
        if valid && mfile.to_slice[valid..].all?(&.zero?)
          mfile.resize(valid)
          # Connected followers get the valid content only this way
          @replicator.try &.replace_file(mfile)
        else
          # An unusable header, or a torn record that an append would only
          # partly overwrite and a later load would read on from
          compact
        end
      end

      # Returns the end of the last whole record, or nil for an unusable header
      private def scan(bytes : Bytes) : Int32?
        return if bytes.size < HEADER_SIZE
        version = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[0, HEADER_SIZE])
        unless version == SCHEMA_VERSION
          Log.warn { "#{@path}: unknown schema version #{version}, recreating" } unless version.zero?
          return
        end
        pos = HEADER_SIZE
        while pos < bytes.size
          state = State.from_value?(bytes[pos]) || break
          len = state.publish_sent? ? PUBLISH_SENT_SIZE : RECORD_SIZE
          break if pos + len > bytes.size
          id = IO::ByteFormat::LittleEndian.decode(UInt16, bytes[pos + 1, 2])
          case state
          in .publish_received? then @awaiting_pubrel << id
          in .pubrel_received?  then @awaiting_pubrel.delete(id)
          in .publish_sent?
            segment = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[pos + 3, 4])
            position = IO::ByteFormat::LittleEndian.decode(UInt32, bytes[pos + 7, 4])
            @publish_sent[id] = SegmentPosition.new(segment, position, 0u32)
          in .pubcomp_received? then @publish_sent.delete(id)
          end
          pos += len
        end
        pos
      end
    end
  end
end
