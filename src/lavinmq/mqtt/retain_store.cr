require "../filesystem"
require "./topic_tree"
require "./protocol"
require "./publish_headers"
require "./consts"
require "../persister"
require "digest/md5"

module LavinMQ
  module MQTT
    class RetainStore
      Log = LavinMQ::Log.for("retainstore")

      # A `.rmsg` file is a header, then the payload. The header is a format
      # byte and its own length (`UInt32`), then the publish timestamp (unix ms,
      # `Int64`) and `AMQP::Properties` carrying the publisher's QoS as
      # `delivery_mode` and the v5 properties as `mqtt.*` headers, all little
      # endian. The length lets a read take the header in one call, since the
      # files are unbuffered.
      MESSAGE_FILE_SUFFIX = ".rmsg"
      FORMAT_VERSION      = 1u8
      FORMAT              = IO::ByteFormat::LittleEndian
      # Written before the header existed: the payload alone. Read as QoS 1,
      # the highest those versions supported, with no properties. Replaced by
      # a `.rmsg` the next time its topic is retained.
      LEGACY_FILE_SUFFIX = ".msg"
      LEGACY_PROPERTIES  = AMQP::Properties.new(headers: RETAINED_HEADERS, delivery_mode: 1u8)
      INDEX_FILE_NAME    = "index"

      alias IndexTree = TopicTree(String)

      # `properties.delivery_mode` is the publisher's QoS; the replay lowers it
      # to the subscription's [MQTT-3.8.4-8].
      record Retained, topic : String, timestamp : Int64, properties : AMQP::Properties,
        body_io : ::IO, bodysize : UInt64

      def initialize(@dir : String, @replicator : Clustering::Replicator?, @index = IndexTree.new, @persister : Persister? = nil)
        FileSystem.mkdir_p @dir
        @files = Hash(String, File).new do |files, file_name|
          file = File.new(File.join(@dir, file_name))
          file.read_buffering = false
          files[file_name] = file
        end
        @index_file_name = File.join(@dir, INDEX_FILE_NAME)
        @index_file = File.new(@index_file_name, "a+")
        @replicator.try &.register_file(@index_file)
        @lock = Mutex.new
        if @index.empty?
          restore_index(@index, @index_file)
          write_index
        end
      end

      def close
        @lock.synchronize do
          write_index
          @index_file.close
          @files.each_value &.close
        end
      end

      private def restore_index(index : IndexTree, index_file : ::IO)
        Log.debug { "restoring index" }
        dir = @dir
        msg_count = 0u64
        msg_file_segments = Set(String).new(
          Dir[Path[dir, "*#{MESSAGE_FILE_SUFFIX}"], Path[dir, "*#{LEGACY_FILE_SUFFIX}"]].compact_map do |fname|
            File.basename(fname)
          end
        )

        while topic = index_file.gets
          # A legacy file left beside its replacement by a crash stays in the
          # set, and is deleted below as unreferenced.
          msg_file_name = make_file_name(topic)
          unless msg_file_segments.delete(msg_file_name)
            msg_file_name = make_file_name(topic, LEGACY_FILE_SUFFIX)
            unless msg_file_segments.delete(msg_file_name)
              Log.warn { "msg file for topic #{topic} missing, dropping from index" }
              next
            end
          end
          index.insert(topic, msg_file_name)
          Log.debug { "restored #{topic}" }
          msg_count += 1
        end

        unless msg_file_segments.empty?
          Log.warn { "unreferenced messages will be deleted: #{msg_file_segments.join(",")}" }
          msg_file_segments.each do |file_name|
            File.delete? File.join(dir, file_name)
          end
        end
        Log.debug { "restoring index done, msg_count = #{msg_count}" }
      end

      def retain(packet : Protocol::Publish) : Nil
        @lock.synchronize do
          topic = packet.topic
          payload = packet.payload
          Log.debug { "retain topic=#{topic} body.bytesize=#{payload.bytesize}" }
          # An empty message with retain flag means clear the topic from retained messages
          # QoS 1 publishes are acked when durable, so like publish confirms
          # they sync what they changed before the persister acks them
          needs_sync = packet.qos > 0
          if payload.empty?
            delete_from_index(topic)
            @persister.try &.mark_dirty(@dir) if needs_sync
            return
          end

          msg_file_name = make_file_name(topic)
          legacy_file_name = nil
          if indexed = @index[topic]?
            if indexed != msg_file_name
              legacy_file_name = indexed
              @index.insert(topic, msg_file_name)
            end
          else
            add_to_index(topic, msg_file_name)
            @persister.try &.mark_dirty(@index_file_name) if needs_sync
          end

          file = File.new(File.join(@dir, "#{msg_file_name}.tmp"), "w+")
          file.sync = true
          file.read_buffering = false
          # sync = true, so this writes straight to the fd, no intermediate
          # buffer and no copy of the payload
          file.write header(packet)
          file.write payload
          final_file_path = File.join(@dir, msg_file_name)
          file.rename(final_file_path)
          @replicator.try &.replace_file(final_file_path)
          # Synced by the persister before the PUBACK, not inline on the read
          # loop while holding @lock
          if needs_sync
            @persister.try &.mark_dirty(final_file_path)
            @persister.try &.mark_dirty(@dir)
          end
          @files.delete(msg_file_name).try &.close
          @files[msg_file_name] = file
          # After the rename, so a crash in between leaves the topic readable.
          delete_file(legacy_file_name) if legacy_file_name
        end
      end

      private def header(packet : Protocol::Publish) : Bytes
        headers = AMQP::Table.new
        headers[RETAIN_HEADER] = true
        PublishHeaders.store(packet.properties, headers)
        properties = AMQP::Properties.new(headers: headers, delivery_mode: packet.qos)
        size = sizeof(Int64) + properties.bytesize
        io = ::IO::Memory.new(1 + sizeof(UInt32) + size)
        io.write_byte FORMAT_VERSION
        io.write_bytes size.to_u32, FORMAT
        io.write_bytes RoughTime.unix_ms, FORMAT
        io.write_bytes properties, FORMAT
        io.to_slice
      end

      # Closes current index file, writes the inmemory index to a tmp file
      # renames the file back to the correct name and replaces it on followers,
      # sets @index_file to the new compacted index file
      private def write_index
        @index_file.close
        f = File.new("#{@index_file_name}.tmp", "w")
        @index.each do |topic|
          f.puts topic
        end
        FileSystem.durable_rename(f, @index_file_name)
        @replicator.try &.replace_file(@index_file_name)
        @index_file = f
      end

      private def add_to_index(topic : String, file_name : String) : Nil
        @index.insert topic, file_name
        line = "#{topic}\n".to_slice
        offset = @index_file.size.to_i64
        @index_file.write line
        @index_file.flush
        @replicator.try &.append_bytes(@index_file_name, line, offset)
      end

      private def delete_from_index(topic : String) : Nil
        if file_name = @index.delete topic
          Log.trace { "deleted '#{topic}' from index, deleting file #{file_name}" }
          delete_file(file_name)
        end
      end

      private def delete_file(file_name : String) : Nil
        path = File.join(@dir, file_name)
        if file = @files.delete(file_name)
          file.close
        end
        File.delete?(path)
        @replicator.try &.delete_file(path)
      end

      # Yields each retained message matching `subscription`, its body
      # positioned at the payload. One past its Message Expiry Interval is
      # discarded instead, so it is no longer the topic's retained message
      # (§3.3.1.3).
      def each(subscription : String, &block : Retained -> Nil) : Nil
        @lock.synchronize do
          # Capacity 0 allocates no buffer until something expires.
          expired = Array(String).new(0)
          @index.each(subscription) do |topic, file_name|
            f = @files[file_name]
            f.rewind
            if file_name.ends_with?(LEGACY_FILE_SUFFIX)
              block.call Retained.new(topic, RoughTime.unix_ms, LEGACY_PROPERTIES, f, f.size.to_u64)
              next
            end
            # Skipped, not raised: this runs in the subscriber's read loop, so
            # one bad file would close every connection subscribing to it.
            timestamp, properties = begin
              read_header(f, file_name)
            rescue ex # a short read, or properties that do not decode
              Log.error(exception: ex) { "skipping unreadable retained message for topic #{topic}" }
              next
            end
            if PublishHeaders.expired?(properties.headers, timestamp)
              expired << topic
              next
            end
            block.call Retained.new(topic, timestamp, properties, f, (f.size - f.pos).to_u64)
          end
          # Not inside the walk, which `delete` would modify.
          expired.each { |topic| delete_from_index(topic) }
        end
      end

      private def read_header(f : File, file_name : String) : {Int64, AMQP::Properties}
        version = f.read_byte
        unless version == FORMAT_VERSION
          raise IO::Error.new("#{file_name}: unknown retained message format #{version.inspect}")
        end
        header = Bytes.new(UInt32.from_io(f, FORMAT))
        f.read_fully(header)
        {FORMAT.decode(Int64, header), AMQP::Properties.from_bytes(header + sizeof(Int64), FORMAT)}
      end

      @hasher = Digest::MD5.new

      private def make_file_name(topic : String, suffix = MESSAGE_FILE_SUFFIX) : String
        @hasher.update topic.to_slice
        "#{@hasher.hexfinal}#{suffix}"
      ensure
        @hasher.reset
      end
    end
  end
end
