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

      # published_at is unix ms: the publish time, or after a restart the
      # modification time of the message file
      record Entry, file_name : String, size : Int64, published_at : Int64

      alias IndexTree = TopicTree(Entry)

      # What a retained publish over a limit does: it is not stored, or the
      # oldest retained messages are removed to make room for it
      enum Overflow
        Reject
        DropHead

        def self.from_setting?(value : String) : self?
          case value
          when "reject"    then Reject
          when "drop-head" then DropHead
          end
        end

        def to_setting : String
          drop_head? ? "drop-head" : "reject"
        end
      end

      # Topics in publish order for drop-head, built on its first use. An entry
      # is stale when its topic was published again or deleted since
      @publish_order : Deque({Int64, String})?

      def initialize(@dir : String, @replicator : Clustering::Replicator?, @index = IndexTree.new,
                     @persister : Persister? = nil, @on_dropped : Proc(Int32, Nil)? = nil)
        FileSystem.mkdir_p @dir
        @index_file_name = File.join(@dir, INDEX_FILE_NAME)
        @index_file = File.new(@index_file_name, "a+")
        @replicator.try &.register_file(@index_file)
        @lock = Mutex.new
        # Set when the index file has lines the index doesn't, by
        # #restore_index or a delete, so it is compacted (see #write_index)
        @dirty = false
        # Lines in the index file for topics that were deleted since it was written
        @dead_lines = 0
        # Written under @lock, read by the HTTP API and metrics from other fibers
        @count = Atomic(Int64).new(0_i64)
        @bytes = Atomic(Int64).new(0_i64)
        if @index.empty?
          restore_index
          write_index if @dirty
        else
          @index.each do |_topic, entry|
            @count.add(1_i64, :relaxed)
            @bytes.add(entry.size, :relaxed)
          end
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
          info = File.info(File.join(dir, msg_file_name))
          @index.insert(topic, Entry.new(msg_file_name, info.size, info.modification_time.to_unix_ms))
          @count.add(1_i64, :relaxed)
          @bytes.add(info.size, :relaxed)
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

      # Returns false, without storing the message, when it would take the
      # store over a limit. A delete is always allowed
      def retain(packet : Protocol::Publish, max_messages : Int64? = nil, max_bytes : Int64? = nil,
                 overflow : Overflow = Overflow::Reject) : Bool
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

          old = @index[topic]?
          old_size = old.try(&.size) || 0_i64
          new_size = payload.bytesize.to_i64
          if over_limit?(old.nil?, old_size, new_size, max_messages, max_bytes)
            return false unless overflow.drop_head?
            return false unless make_room(topic, old.nil?, old_size, new_size, max_messages, max_bytes)
          end

          # The file is written before a new topic is indexed, so a failed
          # write leaves no topic without a message
          entry = Entry.new(old.try(&.file_name) || make_file_name(topic), new_size, RoughTime.unix_ms)
          write_message(entry.file_name, payload, needs_sync)
          if old
            @index.insert(topic, entry)
          else
            add_to_index(topic, entry)
            @persister.try &.mark_dirty(@index_file_name) if needs_sync
          end
          @bytes.add(new_size - old_size, :relaxed)
          track_publish_order(topic, entry.published_at)
          true
        end
      end

      # Removes the oldest retained messages of other topics until the new
      # message fits. Removes nothing for a message that can never fit
      private def make_room(topic : String, new_topic : Bool, old_size : Int64, new_size : Int64,
                            max_messages : Int64?, max_bytes : Int64?) : Bool
        return false if max_bytes && new_size > max_bytes
        return false if new_topic && max_messages == 0
        dropped = 0
        while over_limit?(new_topic, old_size, new_size, max_messages, max_bytes)
          oldest = oldest_topic(except: topic) || break
          delete_from_index(oldest)
          dropped += 1
        end
        if dropped > 0
          @on_dropped.try &.call(dropped)
          compact_index
        end
        !over_limit?(new_topic, old_size, new_size, max_messages, max_bytes)
      end

      private def oldest_topic(except : String) : String?
        order = @publish_order ||= build_publish_order
        while first = order.first?
          published_at, topic = first
          entry = @index[topic]?
          return topic if topic != except && entry && entry.published_at == published_at
          order.shift
        end
      end

      private def build_publish_order : Deque({Int64, String})
        order = Array({Int64, String}).new(@count.get(:relaxed).to_i32)
        @index.each { |topic, entry| order << {entry.published_at, topic} }
        Deque.new(order.sort!)
      end

      private def track_publish_order(topic : String, published_at : Int64) : Nil
        order = @publish_order || return
        order.push({published_at, topic})
        # Mostly stale entries, from topics that are published again and again
        @publish_order = nil if order.size > 2 * @count.get(:relaxed) + 1024
      end

      # Rewrites the index file when most of it is dead lines, so the file
      # can't grow without limit and isn't rewritten for every delete either
      private def compact_index : Nil
        write_index if @dead_lines > @count.get(:relaxed)
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

      # A replacement that doesn't grow is stored also when a lowered limit has
      # left the store over it
      private def over_limit?(new_topic : Bool, old_size : Int64, new_size : Int64,
                              max_messages : Int64?, max_bytes : Int64?) : Bool
        return true if new_topic && max_messages && @count.get(:relaxed) >= max_messages
        return false unless max_bytes && new_size > old_size
        @bytes.get(:relaxed) - old_size + new_size > max_bytes
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
        @dead_lines = 0
      end

      private def add_to_index(topic : String, entry : Entry) : Nil
        @index.insert topic, entry
        @count.add(1_i64, :relaxed)
        line = "#{topic}\n".to_slice
        offset = @index_file.size.to_i64
        @index_file.write line
        @index_file.flush
        @replicator.try &.append_bytes(@index_file_name, line, offset)
      end

      private def delete_from_index(topic : String) : Nil
        if entry = @index.delete topic
          @dirty = true
          @dead_lines += 1
          Log.trace { "deleted '#{topic}' from index, deleting file #{entry.file_name}" }
          file_path = File.join(@dir, entry.file_name)
          @count.sub(1_i64, :relaxed)
          @bytes.sub(entry.size, :relaxed)
          File.delete? file_path
          @replicator.try &.delete_file(file_path)
        end
      end

      def each(subscription : String, &block : String, ::IO, UInt64 -> Nil) : Nil
        @lock.synchronize do
          @index.each(subscription) do |topic, entry|
            File.open(File.join(@dir, entry.file_name)) do |f|
              f.read_buffering = false
              block.call(topic, f, f.size.to_u64)
            end
          end
        end
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
