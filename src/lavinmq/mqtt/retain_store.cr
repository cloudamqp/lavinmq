require "./topic_tree"
require "digest/sha256"

module LavinMQ
  module MQTT
    class RetainStore
      Log = LavinMQ::Log.for("retainstore")

      MESSAGE_FILE_SUFFIX = ".msg"
      INDEX_FILE_NAME     = "index"

      alias IndexTree = TopicTree(String)

      def initialize(@dir : String, @index = IndexTree.new)
        Dir.mkdir_p @dir
        @index_file = File.new(File.join(@dir, INDEX_FILE_NAME), "a+")
        @lock = Mutex.new
        @lock.synchronize do
          if @index.empty?
            restore_index(@index, @index_file)
            write_index
            @index_file = File.new(File.join(@dir, INDEX_FILE_NAME), "a+")
          end
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
        msg_count = 0
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

        # TODO: Device what's the truth: index file or msgs file. Mybe drop the index file and rebuild
        # index from msg files?
        unless msg_file_segments.empty?
          Log.warn { "unreferenced messages will be deleted: #{msg_file_segments.join(",")}" }
          msg_file_segments.each do |msg_file_name|
            File.delete? File.join(dir, msg_file_name)
          end
        end
        # TODO: delete unreferenced messages?
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
          File.rename tmp_file, File.join(@dir, msg_file_name)
        ensure
          FileUtils.rm_rf tmp_file unless tmp_file.nil?
        end
      end

      private def write_index
        tmp_file = File.join(@dir, "#{INDEX_FILE_NAME}.next")
        File.open(tmp_file, "w+") do |f|
          @index.each do |topic, _filename|
            f.puts topic
          end
        end
        File.rename tmp_file, File.join(@dir, INDEX_FILE_NAME)
      ensure
        FileUtils.rm_rf tmp_file unless tmp_file.nil?
      end

      private def add_to_index(topic : String, file_name : String) : Nil
        @index.insert topic, file_name
        @index_file.puts topic
        @index_file.flush
      end

      private def delete_from_index(topic : String) : Nil
        if file_name = @index.delete topic
          Log.trace { "deleted '#{topic}' from index, deleting file #{file_name}" }
          File.delete? File.join(@dir, file_name)
        end
      end

      def each(subscription : String, &block : String, Bytes -> Nil) : Nil
        @lock.synchronize do
          @index.each(subscription) do |topic, file_name|
            block.call(topic, read(file_name))
          end
        end
        nil
      end

      private def read(file_name : String) : Bytes
        File.open(File.join(@dir, file_name), "r") do |f|
          body = Bytes.new(f.size)
          f.read_fully(body)
          body
        end
      end

      def retained_messages
        @lock.synchronize do
          @index.size
        end
      end

      @hasher = Digest::MD5.new

      def make_file_name(topic : String) : String
        @hasher.update topic.to_slice
        hash = @hasher.hexfinal
        @hasher.reset
        "#{hash}#{MESSAGE_FILE_SUFFIX}"
      end
    end
  end
end
