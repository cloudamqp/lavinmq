require "./stream"
require "./stream_consumer"
require "../../mfile"
require "../../schema"
require "./s3_storage_client"
require "./s3_segment_cache"

module LavinMQ::AMQP
  class Stream < DurableQueue
    class S3MessageStore < StreamMessageStore
      @s3_segments = Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)).new
      property storage_client : S3StorageClient
      @segment_cache : S3SegmentCache

      MAX_RUNNING_FIBERS = 8

      def initialize(@msg_dir : String, @replicator : Clustering::Replicator?,
                     durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
        @log = Logger.new(Log, metadata)
        @durable = durable
        @acks = Hash(UInt32, MFile).new
        @consumer_offsets = MFile.new(File.join(@msg_dir, "consumer_offsets"), Config.instance.segment_size)
        @last_offset = 0_i64

        @storage_client = S3StorageClient.new(@msg_dir, metadata)
        @s3_segments = @storage_client.s3_segments_from_bucket || Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)).new
        load_segments_from_disk
        load_stats_from_segments

        @last_offset = get_last_offset
        @rfile_id = @segments.last_key
        @rfile = @segments.last_value
        @wfile_id = @rfile_id
        @wfile = @segments.last_value

        @replicator.try &.register_file @consumer_offsets
        @consumer_offset_positions = restore_consumer_offset_positions

        open_new_segment unless @segment_msg_count[@wfile_id] == 0 # if no messages in the last segment, use it
        @empty.set empty?

        drop_overflow
        @segment_cache = S3SegmentCache.new(@storage_client, @s3_segments, @segments, @msg_dir, metadata)
        @segment_cache.spawn_download_fibers
      end

      private def load_stats_from_segments : Nil
        is_long_queue = (@s3_segments.size + @segments.size) > 255
        @log.info { "Loading: #{@s3_segments.size} segments from S3 and #{@segments.size} local segments" }

        segments = load_stats_from_s3_segments(is_long_queue)
        segments = load_stats_from_local_files(segments, is_long_queue)
        @segments = segments.sort! { |a, b| a[0] <=> b[0] }.to_h
        @log.debug { "Loaded #{segments.size} segments, #{@size} messages" }
        if @segments.empty?
          @log.debug { "No segments found, creating new segment" }
          seg = if @s3_segments.size > 0
                  @s3_segments.last_key + 1
                else
                  1_u32
                end
          path = File.join(@msg_dir, "msgs.#{seg.to_s.rjust(10, '0')}")
          file = MFile.new(path, Config.instance.segment_size)
          file.write_bytes Schema::VERSION
          @replicator.try &.append path, Schema::VERSION
          @segments[seg] = file
        end
      end

      private def load_stats_from_local_files(segments, is_long_queue)
        counter = segments.size
        @segments.each do |seg, mfile|
          if @segment_msg_count[seg].zero? # only load segment if stats not already read from meta file
            @log.debug { "Loading stats for local segment: #{seg}, not uploaded to S3 yet" }
            begin
              read_metadata_file(seg, mfile)
            rescue File::NotFoundError
              mfile.pos = 4
              produce_metadata(seg, mfile)
              write_metadata_file(seg, mfile) unless @segment_msg_count[seg].zero?
            end
          end
          if @segment_msg_count[seg].zero? # don't upload empty segments
            @log.debug { "Deleting empty segment #{seg} from local storage" }
            delete_file(mfile)
            @segments.delete(seg)
          else
            @replicator.try &.register_file mfile
            segments << {seg, mfile}
            upload_segment_to_s3(mfile, seg)
          end
          if is_long_queue
            @log.info { "Loaded #{counter}/#{@s3_segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
          else
            @log.debug { "Loaded #{counter}/#{@s3_segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
          end
          Fiber.yield
        end
        segments
      end

      private def load_stats_from_s3_segments(is_long_queue)
        segments = Array(Tuple(UInt32, MFile)).new(@s3_segments.size)
        counter = 0
        segment_keys = @s3_segments.keys
        keys_mutex = Mutex.new
        wg = WaitGroup.new([MAX_RUNNING_FIBERS, @s3_segments.size].min)
        [MAX_RUNNING_FIBERS, @s3_segments.size].min.times do
          spawn do
            while seg = keys_mutex.synchronize { segment_keys.shift? }
              s3file = @s3_segments[seg]
              path = File.join(Config.instance.data_dir, s3file[:path])
              load_stats_from_meta_file(path, seg, s3file)
              file = verify_local_file?(s3file, seg)
              segments << {seg, file} if file

              # download segment from s3 if meta file was not found and local file is not valid
              # always download last segment unless it exists locally
              if @segment_msg_count[seg].zero? || seg == @s3_segments.last_key
                file ||= @storage_client.download_segment(seg, @s3_segments, @storage_client.http_client)
                if mfile = file
                  @replicator.try &.register_file mfile
                  segments << {seg, mfile}
                  mfile.pos = 4
                  produce_metadata(seg, mfile)
                  write_metadata_file(seg, mfile)
                  slice = Bytes.new(20)
                  File.open(meta_file_name(path), &.read_fully(slice))
                  meta_s3_path = meta_file_name(path[Config.instance.data_dir.bytesize + 1..])
                  @storage_client.upload_file_to_s3("/#{meta_s3_path}", slice)
                else
                  @log.error { "Failed to load segment #{path}" }
                end
              end
              counter += 1
              if is_long_queue
                @log.info { "Loaded #{counter}/#{@s3_segments.size} segments, #{@size} messages" } if counter % 128 == 0
              else
                @log.debug { "Loaded #{counter}/#{@s3_segments.size} segments, #{@size} messages" } if counter % 128 == 0
              end
              Fiber.yield
            end
            wg.done
          end
        end
        wg.wait
        segments
      end

      private def load_stats_from_meta_file(path, seg, s3file)
        meta_path = meta_file_name(path)
        if File.exists?(meta_path)
          read_metadata_file(seg, meta_path, s3file[:size])
          unless s3file[:meta] # upload to s3 unless it exists there
            Log.info { "Uploading metadata file for segment #{seg} to S3" }
            slice = Bytes.new(20)
            File.open(meta_path, &.read_fully(slice))
            meta_s3_path = meta_file_name(path[Config.instance.data_dir.bytesize + 1..])
            @storage_client.upload_file_to_s3("/#{meta_s3_path}", slice)
          end
        elsif meta_file = @storage_client.download_meta_file(seg, @s3_segments, @storage_client.http_client)
          read_metadata_file_from_s3(seg, meta_file)
        end
      end

      private def read_metadata_file(seg, meta_path, bytesize)
        return unless File.exists?(meta_path)
        File.open(meta_path) do |file|
          read_metadata(file, seg)
          @bytesize += bytesize
        end
      end

      private def read_metadata_file_from_s3(seg, mfile)
        read_metadata(mfile, seg)
        mfile.dontneed
        @bytesize += @s3_segments[seg][:size] - 4
      end

      private def read_metadata(file, seg)
        count = file.read_bytes(UInt32)
        @offset_index[seg] = file.read_bytes(Int64)
        @timestamp_index[seg] = file.read_bytes(Int64)
        @segment_msg_count[seg] = count
        @size += count unless seg == @s3_segments.last_key # will be added when reading file later
        @log.debug { "Reading metadata from #{file.path}: #{count} msgs" }
      end

      private def verify_local_file?(s3file, seg_id) : MFile | Nil
        path = File.join(Config.instance.data_dir, s3file[:path])
        return unless File.exists?(path)
        mfile = @segments[seg_id]

        digest = Digest::MD5.new
        digest.update mfile.to_slice

        @segments.delete(seg_id)
        unless (local_hash = digest.hexfinal) == s3file[:etag]
          @log.debug { "Local file #{path} has different etag than S3: #{local_hash} != #{s3file[:etag]}" }
          File.delete(path)
          return
        end
        @log.debug { "Loading local file: #{path}" }
        mfile
      end

      def add_consumer(tag, segment)
        @segment_cache.current_read_segments[tag] = segment
      end

      def remove_consumer(tag)
        @segment_cache.current_read_segments.delete(tag)
      end

      # Wait for download of the next segment if needed
      private def next_segment(consumer) : MFile?
        consumer.segment += 1
        @segment_cache.current_read_segments[consumer.tag] = consumer.segment
        consumer.pos = 4u32

        unless @segments[consumer.segment]?
          return unless @s3_segments[consumer.segment]?
          counter = 0
          while !@segments[consumer.segment]?
            sleep 10.milliseconds
            if (counter &+= 1) % 128 == 0
              @log.info { "Waiting for segment #{consumer.segment} to be downloaded" }
            end
          end
        end
        @segments[consumer.segment]
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        if !@wfile_id.zero? && !@segment_msg_count[@wfile_id].zero?
          write_metadata_file(@wfile_id, @wfile)
          @wfile.truncate(@wfile.size)
        end
        next_id = @wfile_id + 1
        path = File.join(@msg_dir, "msgs.#{next_id.to_s.rjust(10, '0')}")
        capacity = Math.max(Config.instance.segment_size, next_msg_size + 4)
        wfile = MFile.new(path, capacity)
        wfile.write_bytes Schema::VERSION
        wfile.pos = 4
        @replicator.try &.register_file wfile
        @replicator.try &.append path, Schema::VERSION
        @wfile_id = next_id
        @wfile = @segments[@wfile_id] = wfile

        drop_overflow
        @offset_index[@wfile_id] = @last_offset + 1
        @timestamp_index[@wfile_id] = RoughTime.unix_ms

        delete_unused_segments
        upload_missing_segments_to_s3
        wfile
      rescue ex
        @log.error { "Failed to open new segment: #{ex}" }
        raise ex
      end

      private def upload_missing_segments_to_s3
        @segments.reject { |seg_id, _s| seg_id == @wfile_id || @s3_segments[seg_id]? }.each do |seg_id, segment|
          upload_segment_to_s3(segment, seg_id)
        end
      end

      private def upload_segment_to_s3(segment, seg_id)
        return if @s3_segments[seg_id]?
        return unless @segment_msg_count[seg_id] > 0 # don't upload empty segments
        @log.debug { "Uploading file to s3: /#{segment.path[Config.instance.data_dir.bytesize + 1..]}" }
        path = "/#{segment.path[Config.instance.data_dir.bytesize + 1..]}"
        etag = @storage_client.upload_file_to_s3(path, segment.to_slice)
        meta_path = meta_file_name(segment.path)
        if File.exists?(meta_path)
          @storage_client.upload_file_to_s3(meta_file_name(path), File.open(meta_path, &.getb_to_end))
        end

        @s3_segments[seg_id] = {
          path: path[1..],
          etag: etag,
          size: segment.size,
          meta: true,
        }
      end

      private def drop_segments_while(& : UInt32 -> Bool)
        @s3_segments.reject! do |seg_id, s3_seg|
          should_drop = yield seg_id
          break unless should_drop
          next if seg_id == @wfile_id # never delete the last active segment
          msg_count = @segment_msg_count.delete(seg_id)
          @size -= msg_count if msg_count
          @segment_last_ts.delete(seg_id)
          @offset_index.delete(seg_id)
          @timestamp_index.delete(seg_id)
          @bytesize -= s3_seg[:size] - 4
          @storage_client.delete_from_s3(s3_seg)
          if mfile = @segments.delete(seg_id)
            delete_file(mfile)
          end
          true
        end
      end

      def delete
        super
        @s3_segments.reject! do |_seg_id, s3_seg|
          @storage_client.delete_from_s3(s3_seg)
        end
      end

      # Download segment if needed
      private def find_offset_in_segments(offset : Int | Time, retries = 5) : Tuple(Int64, UInt32, UInt32)
        segment = offset_index_lookup(offset)
        unless @segments[segment]?
          if mfile = @storage_client.download_segment(segment, @s3_segments)
            @segments[segment] = mfile
          else
            if (retries -= 1) <= 0
              raise "Segment #{segment} not found in S3 and could not be downloaded"
            else
              @log.warn { "Segment #{segment} not found in S3, retrying download" }
              find_offset_in_segments(offset, retries)
            end
          end
        end
        super(offset)
      end
    end
  end
end
