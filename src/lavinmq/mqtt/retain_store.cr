require "./topic_tree"
require "digest/md5"

module LavinMQ
  module MQTT
    class RetainStore
      Log = LavinMQ::Log.for("retainstore")

      MESSAGE_FILE_SUFFIX = ".msg"
      INDEX_FILE_NAME     = "index"

      alias IndexTree = TopicTree(String)

      def initialize(@dir : String, @replicator : Clustering::Replicator, @index = IndexTree.new)
        Dir.mkdir_p @dir
        @files = Hash(String, File).new do |files, file_name|
          files[file_name] = File.open(File.join(@dir, file_name), "W").tap &.sync = true
        end
        @index_file_name = File.join(@dir, INDEX_FILE_NAME)
        @index_file = File.new(@index_file_name, "a+")
        @replicator.register_file(@index_file)
        @lock = Mutex.new
        if @index.empty?
          restore_index(@index, @index_file)
          write_index
          @index_file = File.new(@index_file_name, "a+")
        end
      end

      def close
        @lock.synchronize do
          write_index
        end
      end

      private def restore_index(index : IndexTree, index_file : ::IO)
        Log.info { "restoring index" }
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
        Log.info { "restoring index done, msg_count = #{msg_count}" }
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

          tmp_file = File.join(@dir, "#{msg_file_name}.tmp")
          File.open(tmp_file, "w+") do |f|
            f.sync = true
            ::IO.copy(body_io, f)
          end
          final_file_path = File.join(@dir, msg_file_name)
          File.rename(tmp_file, final_file_path)
          @files.delete(final_file_path)
          @files[msg_file_name] = File.new(final_file_path, "r+")
          @replicator.replace_file(final_file_path)
        ensure
          FileUtils.rm_rf tmp_file unless tmp_file.nil?
        end
      end

      private def write_index
        File.open("#{@index_file_name}.tmp", "w") do |f|
          @index.each do |topic|
            f.puts topic
          end
          f.rename @index_file_name
        end
        @replicator.replace_file(@index_file_name)
      rescue ex
        FileUtils.rm_rf File.join(@dir, "#{INDEX_FILE_NAME}.tmp")
        raise ex
      end

      private def add_to_index(topic : String, file_name : String) : Nil
        @index.insert topic, file_name
        @index_file.puts topic
        @replicator.append(@index_file_name, "#{topic}\n".to_slice)
      end

      private def delete_from_index(topic : String) : Nil
        if file_name = @index.delete topic
          Log.trace { "deleted '#{topic}' from index, deleting file #{file_name}" }
          if file = @files.delete(file_name)
            file.close
            file.delete
          end
          @replicator.delete_file(File.join(@dir, file_name))
        end
      end

      def each(subscription : String, &block : String, ::IO, UInt64 -> Nil) : Nil
        @lock.synchronize do
          @index.each(subscription) do |topic, file_name|
            io = ::IO::Memory.new(read(file_name))
            block.call(topic, io, io.bytesize.to_u64)
          end
        end
      end

      private def read(file_name : String) : Bytes
        f = @files[file_name]
        body = Bytes.new(f.size)
        f.read_fully(body)
        body
      end

      def retained_messages
        @lock.synchronize do
          @index.size
        end
      end

      @hasher = Digest::MD5.new

      def make_file_name(topic : String) : String
        @hasher.update topic.to_slice
        "#{@hasher.hexfinal}#{MESSAGE_FILE_SUFFIX}"
      ensure
        @hasher.reset
      end
    end
  end
end
