require "./topic_tree"

module LavinMQ
  class RetainStore
    Log = MyraMQ::Log.for("retainstore")

    RETAINED_STORE_DIR_NAME = "retained"
    MESSAGE_FILE_SUFFIX     = ".msg"
    INDEX_FILE_NAME         = "index"

    # This is a helper strut to read and write retain index entries
    struct RetainIndexEntry
      getter topic, sp

      def initialize(@topic : String, @sp : SegmentPosition)
      end

      def to_io(io : IO)
        io.write_bytes @topic.bytesize.to_u16, ::IO::ByteFormat::NetworkEndian
        io.write @topic.to_slice
        @sp.to_io(io, ::IO::ByteFormat::NetworkEndian)
      end

      def self.from_io(io : IO)
        topic_length = UInt16.from_io(io, ::IO::ByteFormat::NetworkEndian)
        topic = io.read_string(topic_length)
        sp = SegmentPosition.from_io(io, ::IO::ByteFormat::NetworkEndian)
        self.new(topic, sp)
      end
    end

    alias IndexTree = TopicTree(RetainIndexEntry)

    # TODO: change frm "data_dir" to "retained_dir", i.e. pass
    # equivalent of File.join(data_dir, RETAINED_STORE_DIR_NAME) as
    # data_dir.
    def initialize(data_dir : String, @index = IndexTree.new, index_file : IO? = nil)
      @dir = File.join(data_dir, RETAINED_STORE_DIR_NAME)
      Dir.mkdir_p @dir

      @index_file = index_file || File.new(File.join(@dir, INDEX_FILE_NAME), "a+")
      if (buffered_index_file = @index_file).is_a?(IO::Buffered)
        buffered_index_file.sync = true
      end

      @segment = 0u64

      @lock = Mutex.new
      @lock.synchronize do
        if @index.empty?
          restore_index(@index, @index_file)
          write_index
        end
      end
    end

    def close
      @lock.synchronize do
        write_index
      end
    end

    private def restore_index(index : IndexTree, index_file : IO)
      Log.info { "restoring index" }
      # If @segment is greater than zero we've already restored index or have been
      # writing messages
      raise "restore_index: can't restore, @segment=#{@segment} > 0" unless @segment.zero?
      dir = @dir

      # Create a set with all segment positions based on msg files
      msg_file_segments = Set(UInt64).new(
        Dir[Path[dir, "*#{MESSAGE_FILE_SUFFIX}"]].compact_map do |fname|
          File.basename(fname, MESSAGE_FILE_SUFFIX).to_u64?
        end
      )

      segment = 0u64
      msg_count = 0
      loop do
        entry = RetainIndexEntry.from_io(index_file)

        unless msg_file_segments.delete(entry.sp.to_u64)
          Log.warn { "msg file for topic #{entry.topic} missing, dropping from index" }
          next
        end

        index.insert(entry.topic, entry)
        segment = Math.max(segment, entry.sp.to_u64 + 1)
        Log.debug { "restored #{entry.topic}" }
        msg_count += 1
      rescue IO::EOFError
        break
      end

      # TODO: Device what's the truth: index file or msgs file. Mybe drop the index file and rebuild
      # index from msg files?
      unless msg_file_segments.empty?
        Log.warn { "unreferenced messages: " \
                   "#{msg_file_segments.map { |u| "#{u}#{MESSAGE_FILE_SUFFIX}" }.join(",")}" }
      end

      @segment = segment
      Log.info { "restoring index done, @segment = #{@segment}, msg_count = #{msg_count}" }
    end

    def retain(topic : String, body : Bytes) : Nil
      @lock.synchronize do
        Log.trace { "retain topic=#{topic} body.bytesize=#{body.bytesize}" }

        # An empty message with retain flag means clear the topic from retained messages
        if body.bytesize.zero?
          delete_from_index(topic)
          return
        end

        sp = if entry = @index[topic]?
               entry.sp
             else
               new_sp = SegmentPosition.new(@segment)
               @segment += 1
               add_to_index(topic, new_sp)
               new_sp
             end

        File.open(File.join(@dir, self.class.make_file_name(sp)), "w+") do |f|
          f.sync = true
          MessageData.new(topic, body).to_io(f)
        end
      end
    end

    private def write_index
      tmp_file = File.join(@dir, "#{INDEX_FILE_NAME}.next")
      File.open(tmp_file, "w+") do |f|
        @index.each do |entry|
          entry.to_io(f)
        end
      end
      File.rename tmp_file, File.join(@dir, INDEX_FILE_NAME)
    ensure
      FileUtils.rm_rf tmp_file unless tmp_file.nil?
    end

    private def add_to_index(topic : String, sp : SegmentPosition) : Nil
      entry = RetainIndexEntry.new(topic, sp)
      @index.insert topic, entry
      entry.to_io(@index_file)
    end

    private def delete_from_index(topic : String) : Nil
      if entry = @index.delete topic
        file_name = self.class.make_file_name(entry.sp)
        Log.trace { "deleted '#{topic}' from index, deleting file #{file_name}" }
        File.delete? File.join(@dir, file_name)
      end
    end

    def each(subscription : String, &block : String, SegmentPosition -> Nil) : Nil
      @lock.synchronize do
        @index.each(subscription) do |entry|
          block.call(entry.topic, entry.sp)
        end
      end
      nil
    end

    def read(sp : SegmentPosition) : MessageData?
      unless sp.retain?
        Log.error { "can't handle sp with retain=true" }
        return
      end
      @lock.synchronize do
        read_message_file(sp)
      end
    end

    def retained_messages
      @lock.synchronize do
        @index.size
      end
    end

    @[AlwaysInline]
    private def read_message_file(sp : SegmentPosition) : MessageData?
      file_name = self.class.make_file_name(sp)
      file = File.join @dir, file_name
      File.open(file, "r") do |f|
        MessageData.from_io(f)
      end
    rescue e : File::NotFoundError
      Log.error { "message file #{file_name} doesn't exist" }
      nil
    end

    @[AlwaysInline]
    def self.make_file_name(sp : SegmentPosition) : String
      "#{sp.to_u64}#{MESSAGE_FILE_SUFFIX}"
    end
  end
end
