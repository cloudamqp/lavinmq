require "./topic_tree"
require "digest/md5"

module LavinMQ
  module MQTT
    class RetainStore
      Log = LavinMQ::Log.for("retainstore")

      MESSAGE_FILE_SUFFIX = ".msg"
      INDEX_FILE_NAME     = "index"

      alias IndexTree = TopicTree(String)

      def initialize(@dir : String, @replicator : Clustering::Replicator?, @index = IndexTree.new)
        Dir.mkdir_p @dir
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
          Dir[Path[dir, "*#{MESSAGE_FILE_SUFFIX}"]].compact_map do |fname|
            File.basename(fname)
          end
        )

        while topic = index_file.gets
          msg_file_name = make_file_name(topic)
          unless msg_file_segments.delete(msg_file_name)
            Log.warn { "msg file for topic #{topic} missing, dropping from index" }
            next
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

      def retain(topic : String, body_io : ::IO, size : UInt64) : Nil
        @lock.synchronize do
          Log.debug { "retain topic=#{topic} body.bytesize=#{size}" }
          # An empty message with retain flag means clear the topic from retained messages
          if size.zero?
            delete_from_index(topic)
            return
          end

          unless msg_file_name = @index[topic]?
            msg_file_name = make_file_name(topic)
            add_to_index(topic, msg_file_name)
          end

          file = File.new(File.join(@dir, "#{msg_file_name}.tmp"), "w+")
          file.sync = true
          file.read_buffering = false
          len = ::IO.copy(body_io, file, size)
          raise ::IO::EOFError.new("Copied only #{len} of #{size} bytes") if len != size
          final_file_path = File.join(@dir, msg_file_name)
          file.rename(final_file_path)
          @replicator.try &.replace_file(final_file_path)
          @files.delete(msg_file_name).try &.close
          @files[msg_file_name] = file
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
        f.flush
        f.rename @index_file_name
        @replicator.try &.replace_file(@index_file_name)
        @index_file = f
      end

      private def add_to_index(topic : String, file_name : String) : Nil
        @index.insert topic, file_name
        @index_file.puts topic
        @index_file.flush
        @replicator.try &.append(@index_file_name, "#{topic}\n".to_slice)
      end

      private def delete_from_index(topic : String) : Nil
        if file_name = @index.delete topic
          Log.trace { "deleted '#{topic}' from index, deleting file #{file_name}" }
          if file = @files.delete(file_name)
            file.close
            file.delete
          end
          @replicator.try &.delete_file(File.join(@dir, file_name), WaitGroup.new)
        end
      end

      def each(subscription : String, &block : String, ::IO, UInt64 -> Nil) : Nil
        @lock.synchronize do
          @index.each(subscription) do |topic, file_name|
            f = @files[file_name]
            f.rewind
            block.call(topic, f, f.size.to_u64)
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
