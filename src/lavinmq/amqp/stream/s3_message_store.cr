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
      getter storage_client : S3StorageClient
      @segment_cache : S3SegmentCache?

      MAX_RUNNING_FIBERS = 8

      def initialize(@msg_dir : String, @replicator : Clustering::Replicator?,
                     durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
        @storage_client = S3StorageClient.new(@msg_dir, metadata)
        @s3_segments = @storage_client.s3_segments_from_bucket

        # Ensure last S3 segment and all meta files are local before base init
        prepare_local_files_from_s3

        super # MessageStore + StreamMessageStore init (loads local segments/stats)

        # Load stats for S3-only segments (those without local MFiles)
        load_s3_only_segment_stats

        # Recalculate after loading S3 segment stats
        @last_offset = get_last_offset

        # Open a new segment if the current one has messages.
        # Downloaded S3 segments are complete and shouldn't be appended to.
        # This triggers open_new_segment which uploads the previous segment.
        open_new_segment unless @segment_msg_count[@wfile_id].zero?

        # Upload any remaining local segments not yet in S3
        upload_missing_segments_to_s3

        @segment_cache = S3SegmentCache.new(@storage_client, @s3_segments, @segments, @msg_dir, metadata)
      end

      private def open_new_segment(next_msg_size = 0) : MFile
        prev_seg_id = @wfile_id unless @wfile_id.zero? || @segment_msg_count[@wfile_id]?.try(&.zero?)
        super.tap do
          if prev_seg_id
            spawn(name: "S3MessageStore#upload-segment-#{prev_seg_id}") do
              3.times do |attempt|
                upload_segment_to_s3(prev_seg_id)
                break
              rescue ex
                if attempt < 2
                  @log.warn { "Failed to upload segment #{prev_seg_id} to S3 (attempt #{attempt + 1}/3): #{ex.message}" }
                  sleep (attempt + 1).seconds
                else
                  @log.error { "Failed to upload segment #{prev_seg_id} to S3 after 3 attempts: #{ex.message}" }
                end
              end
            end
          end
        end
      end

      private def download_segment(seg_id : UInt32) : MFile?
        return nil unless @s3_segments[seg_id]?

        # Try the segment cache first (may already be downloading)
        if cache = @segment_cache
          if mfile = cache.wait_for_segment(seg_id)
            return mfile
          end
        end

        # Direct download as fallback
        @storage_client.with_http_client(with_timeouts: true) do |h|
          if mfile = @storage_client.download_segment(seg_id, @s3_segments, h)
            @segments[seg_id] = mfile
            return mfile
          end
        end
        # Another fiber may have downloaded it concurrently
        @segments[seg_id]?
      end

      # -- Overrides --

      def next_segment_id(segment) : UInt32?
        local = super
        s3 = @s3_segments.each_key.find { |sid| sid > segment }
        case {local, s3}
        when {UInt32, UInt32} then Math.min(local, s3)
        when {UInt32, nil}    then local
        when {nil, UInt32}    then s3
        else                       nil
        end
      end

      def close : Nil
        @segment_cache.try &.close
        super
      end

      def delete
        super
        @s3_segments.each_value do |s3_seg|
          @storage_client.delete_from_s3(s3_seg)
        end
        @s3_segments.clear
      end

      def add_consumer(tag, segment)
        if cache = @segment_cache
          cache.notify_consumer_added
          cache.current_read_segments[tag] = segment
          cache.ensure_fibers_running
        end
      end

      def remove_consumer(tag)
        if cache = @segment_cache
          cache.current_read_segments.delete(tag)
          cache.notify_consumer_removed
        end
      end

      private def drop_segments_while(& : UInt32 -> Bool)
        # Iterate segment_msg_count (the authoritative index of all segments,
        # including those not yet uploaded to S3) instead of just @s3_segments
        @segment_msg_count.reject! do |seg_id, msg_count|
          should_drop = yield seg_id
          break unless should_drop
          next if seg_id == @wfile_id # never delete the active segment

          @size -= msg_count
          @segment_last_ts.delete(seg_id)
          @segment_first_offset.delete(seg_id)
          @segment_first_ts.delete(seg_id)

          if s3_seg = @s3_segments.delete(seg_id)
            @bytesize -= s3_seg[:size] - 4
            @storage_client.delete_from_s3(s3_seg)
          elsif mfile = @segments[seg_id]?
            @bytesize -= mfile.size - 4
          end

          if mfile = @segments.delete(seg_id)
            delete_file(mfile, including_meta: true)
          end
          true
        end
      end

      # -- S3-specific private methods --

      # Download meta files and last segment from S3 so base init can process them
      private def prepare_local_files_from_s3
        return if @s3_segments.empty?

        # Download meta files (each gets its own client to avoid connection issues)
        @s3_segments.each do |seg_id, s3_seg|
          next unless s3_seg[:meta] # only download if meta exists in S3
          meta_path = File.join(@msg_dir, "meta.#{seg_id.to_s.rjust(10, '0')}")
          next if File.exists?(meta_path)
          @storage_client.with_http_client(with_timeouts: true) do |h|
            @storage_client.download_meta_file(seg_id, @s3_segments, h)
          end
        end

        # Ensure last S3 segment is local (we need it for writing)
        last_seg = @s3_segments.each_key.max
        local_path = File.join(@msg_dir, "msgs.#{last_seg.to_s.rjust(10, '0')}")
        unless File.exists?(local_path)
          @storage_client.with_http_client(with_timeouts: true) do |h|
            @storage_client.download_segment(last_seg, @s3_segments, h)
          end
        end
      end

      # Load stats for S3 segments that don't have local MFiles
      # (Base init only loaded stats for segments in @segments)
      private def load_s3_only_segment_stats
        @s3_segments.each do |seg_id, s3_seg|
          next if @segment_msg_count[seg_id]? && !@segment_msg_count[seg_id].zero?

          meta_path = File.join(@msg_dir, "meta.#{seg_id.to_s.rjust(10, '0')}")
          if File.exists?(meta_path)
            read_s3_meta_file(seg_id, meta_path, s3_seg[:size])
          else
            # No meta file available, download segment and produce metadata
            @storage_client.with_http_client(with_timeouts: true) do |h|
              if mfile = @storage_client.download_segment(seg_id, @s3_segments, h)
                @segments[seg_id] = mfile
                mfile.pos = 4
                produce_metadata(seg_id, mfile)
                write_metadata_file(seg_id, mfile)
                upload_meta_to_s3(seg_id, mfile)
              end
            end
          end
        end
      end

      private def read_s3_meta_file(seg_id, meta_path, s3_size)
        File.open(meta_path) do |file|
          count = file.read_bytes(UInt32)
          @segment_msg_count[seg_id] = count
          @segment_first_offset[seg_id] = file.read_bytes(Int64)
          @segment_first_ts[seg_id] = file.read_bytes(Int64)
          begin
            @segment_last_ts[seg_id] = file.read_bytes(Int64)
          rescue IO::EOFError
            # Older meta format without last_ts
          end
          @size += count
          @bytesize += s3_size - 4
        end
      end

      private def upload_segment_to_s3(seg_id : UInt32)
        return if @s3_segments[seg_id]?
        return unless @segment_msg_count[seg_id]? && @segment_msg_count[seg_id] > 0

        mfile = @segments[seg_id]? || return
        s3_path = "/#{@storage_client.s3_path(mfile.path)}"
        @log.debug { "Uploading segment to S3: #{s3_path}" }
        etag = @storage_client.upload_file_to_s3(s3_path, mfile.to_slice)

        upload_meta_to_s3(seg_id, mfile)

        @s3_segments[seg_id] = {
          path: s3_path[1..],
          etag: etag,
          size: mfile.size,
          meta: true,
        }
      end

      private def upload_meta_to_s3(seg_id : UInt32, mfile : MFile)
        meta_path = meta_file_name(mfile)
        if File.exists?(meta_path)
          s3_meta_path = "/#{@storage_client.s3_path(meta_path)}"
          @storage_client.upload_file_to_s3(s3_meta_path, File.open(meta_path, &.getb_to_end))
        end
      end

      private def upload_missing_segments_to_s3
        @segments.each do |seg_id, _mfile|
          next if seg_id == @wfile_id
          upload_segment_to_s3(seg_id)
        end
      end
    end
  end
end
