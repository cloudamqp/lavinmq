require "../filesystem"
require "./consts"
require "./topic_tree"
require "./protocol"
require "../persister"
require "digest/md5"

module LavinMQ
  module MQTT
    class RetainStore
      Log = LavinMQ::Log.for("retainstore")

      MESSAGE_FILE_SUFFIX = ".msg"
      INDEX_FILE_NAME     = "index"

      alias IndexTree = TopicTree(String)

      def initialize(@dir : String, @replicator : Clustering::Replicator?, @index = IndexTree.new, @persister : Persister? = nil)
        FileSystem.mkdir_p @dir
        @index_file_name = File.join(@dir, INDEX_FILE_NAME)
        @index_file = File.new(@index_file_name, "a+")
        @replicator.try &.register_file(@index_file)
        @lock = Mutex.new
        # Set when the index file has lines the index doesn't, by
        # #restore_index or a delete, so it is compacted (see #write_index)
        @dirty = false
        # Written under @lock, read by the HTTP API and metrics from other fibers
        @count = Atomic(Int64).new(0_i64)
        @bytes = Atomic(Int64).new(0_i64)
        if @index.empty?
          restore_index
          write_index if @dirty
        end
      end

      def message_count : Int64
        @count.get(:relaxed)
      end

      def bytesize : Int64
        @bytes.get(:relaxed)
      end

      def close
        @lock.synchronize do
          write_index if @dirty
          @index_file.close
        end
      end

      # Marks the index file dirty when it needs compacting: lines it has that
      # weren't restored (dropped topics, repeated topics), or a last line a
      # crash left without its newline, which the next append would join
      private def restore_index : Nil
        Log.debug { "restoring index" }
        dir = @dir
        msg_count = 0u64
        lines = 0
        msg_file_segments = Set(String).new(
          Dir[Path[dir, "*#{MESSAGE_FILE_SUFFIX}"]].compact_map do |fname|
            File.basename(fname)
          end
        )

        torn = false
        while line = @index_file.gets(chomp: false)
          lines += 1
          torn = !line.ends_with?('\n')
          topic = line.chomp
          if topic.bytesize > MAX_TOPIC_BYTESIZE
            Log.warn { "dropping retained message, topic is #{topic.bytesize} bytes, max #{MAX_TOPIC_BYTESIZE}" }
            next
          end
          msg_file_name = make_file_name(topic)
          unless msg_file_segments.delete(msg_file_name)
            Log.warn { "msg file for topic #{topic} missing, dropping from index" }
            next
          end
          @index.insert(topic, msg_file_name)
          @count.add(1_i64, :relaxed)
          @bytes.add(stored_size(msg_file_name), :relaxed)
          @replicator.try &.register_file(File.join(dir, msg_file_name))
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
        @dirty = torn || lines != @index.size
      end

      # Returns false, without storing the message, when it is over a limit.
      # A delete is always allowed
      def retain(packet : Protocol::Publish, max_messages : Int64? = nil, max_message_size : Int64? = nil) : Bool
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
            return true
          end

          msg_file_name = @index[topic]?
          old_size = msg_file_name ? stored_size(msg_file_name) : 0_i64
          new_size = payload.bytesize.to_i64
          return false if max_message_size && new_size > max_message_size
          # A replacement is not a new message
          return false if msg_file_name.nil? && max_messages && @count.get(:relaxed) >= max_messages

          # The file is written before a new topic is indexed, so a failed
          # write leaves no topic without a message
          if msg_file_name
            write_message(msg_file_name, payload, needs_sync)
          else
            msg_file_name = make_file_name(topic)
            write_message(msg_file_name, payload, needs_sync)
            add_to_index(topic, msg_file_name)
            @persister.try &.mark_dirty(@index_file_name) if needs_sync
          end
          @bytes.add(new_size - old_size, :relaxed)
          true
        end
      end

      private def write_message(file_name : String, payload : Bytes, needs_sync : Bool) : Nil
        tmp_file_path = File.join(@dir, "#{file_name}.tmp")
        final_file_path = File.join(@dir, file_name)
        File.open(tmp_file_path, "w+") do |file|
          file.sync = true
          file.read_buffering = false
          # sync = true, so this writes the payload straight to the fd, no
          # intermediate buffer and no copy
          file.write payload
          file.rename(final_file_path)
        rescue ex
          File.delete?(tmp_file_path)
          raise ex
        end
        @replicator.try &.replace_file(final_file_path)
        # Synced by the persister before the PUBACK, not inline on the read
        # loop while holding @lock
        if needs_sync
          @persister.try &.mark_dirty(final_file_path)
          @persister.try &.mark_dirty(@dir)
        end
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
        @dirty = false
      end

      private def add_to_index(topic : String, file_name : String) : Nil
        @index.insert topic, file_name
        @count.add(1_i64, :relaxed)
        line = "#{topic}\n".to_slice
        offset = @index_file.size.to_i64
        @index_file.write line
        @index_file.flush
        @replicator.try &.append_bytes(@index_file_name, line, offset)
      end

      private def delete_from_index(topic : String) : Nil
        if file_name = @index.delete topic
          @dirty = true
          Log.trace { "deleted '#{topic}' from index, deleting file #{file_name}" }
          file_path = File.join(@dir, file_name)
          @count.sub(1_i64, :relaxed)
          @bytes.sub(stored_size(file_name), :relaxed)
          File.delete? file_path
          @replicator.try &.delete_file(file_path)
        end
      end

      def each(subscription : String, &block : String, ::IO, UInt64 -> Nil) : Nil
        @lock.synchronize do
          @index.each(subscription) do |topic, file_name|
            File.open(File.join(@dir, file_name)) do |f|
              f.read_buffering = false
              block.call(topic, f, f.size.to_u64)
            end
          end
        end
      end

      private def stored_size(file_name : String) : Int64
        File.info?(File.join(@dir, file_name)).try(&.size) || 0_i64
      end

      @hasher = Digest::MD5.new

      private def make_file_name(topic : String) : String
        @hasher.update topic.to_slice
        "#{@hasher.hexfinal}#{MESSAGE_FILE_SUFFIX}"
      ensure
        @hasher.reset
      end
    end
  end
end
