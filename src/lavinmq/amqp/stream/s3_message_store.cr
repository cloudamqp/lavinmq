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
      @storage_client : S3StorageClient
      @segment_cache : S3SegmentCache?
      @failed_uploads = Deque(UInt32).new
      @upload_queue = ::Channel(UInt32).new
      @catalog_io : File?
      @catalog_data : Hash(UInt32, NamedTuple(msg_count: UInt32, first_offset: Int64, first_ts: Int64, last_ts: Int64))?
      @catalog_dirty = false
      @last_catalog_upload = Time.instant
      NUM_UPLOAD_WORKERS      =  4
      CATALOG_RECORD_SIZE     = 32
      CATALOG_UPLOAD_INTERVAL = 10.seconds

      def initialize(@msg_dir : String, @replicator : Clustering::Replicator?,
                     durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
        @storage_client = S3StorageClient.new(@msg_dir, metadata)
        @s3_segments = @storage_client.s3_segments_from_bucket

        # Load segment catalog (1 GET instead of N meta downloads)
        load_segment_catalog

        # Ensure last S3 segment and remaining meta files are local before base init
        prepare_local_files_from_s3

        super # MessageStore + StreamMessageStore init (loads local segments/stats)

        # Load stats for S3-only segments (those without local MFiles)
        load_s3_only_segment_stats
        @catalog_data = nil # Free catalog data after loading stats

        # Re-sort segment hashes so first_key/last_key work correctly.
        # Base init inserts local segments first; S3-only segments are appended
        # after, breaking the insertion-order == segment-order assumption.
        sort_segment_hashes!

        # Fix up the write segment's first_offset — base init set it using
        # @last_offset before S3 segment stats were loaded, so it's wrong.
        if @segment_msg_count[@wfile_id].zero?
          prev_seg = @segment_first_offset.each_key.select { |k| k < @wfile_id }.max?
          if prev_seg
            @segment_first_offset[@wfile_id] = @segment_first_offset[prev_seg] + @segment_msg_count[prev_seg]
          end
        end

        # Recalculate after loading S3 segment stats
        @last_offset = get_last_offset
        @log.info { "Loaded #{@segment_msg_count.size} segments, #{@size} messages (#{@s3_segments.size} in S3)" }

        start_upload_workers

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
          # Re-enqueue any previously failed uploads first
          while failed_id = @failed_uploads.shift?
            @upload_queue.send(failed_id)
          end
          if prev_seg_id
            @upload_queue.send(prev_seg_id)
          end
        end
      end

      private def start_upload_workers
        NUM_UPLOAD_WORKERS.times do |i|
          spawn(name: "S3MessageStore#upload-worker-#{i}") do
            upload_worker(i)
          end
        end
      end

      private def upload_worker(id : Int32)
        @storage_client.with_http_client(with_timeouts: false) do |h|
          loop do
            seg_id = @upload_queue.receive? || break
            upload_segment_with_retry(seg_id, h)
            if cache = @segment_cache
              cache.ensure_fibers_running
              cache.notify_upload_complete
            end
          end
        end
      rescue ::Channel::ClosedError
      end

      private def upload_segment_with_retry(seg_id : UInt32, h : ::HTTP::Client)
        3.times do |attempt|
          upload_segment_to_s3(seg_id, h)
          return
        rescue ex
          if attempt < 2
            @log.warn { "Failed to upload segment #{seg_id} to S3 (attempt #{attempt + 1}/3): #{ex.message}" }
          else
            @log.error { "Failed to upload segment #{seg_id} to S3 after 3 attempts, will retry on next rotation: #{ex.message}" }
            @failed_uploads << seg_id unless @failed_uploads.includes?(seg_id)
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

        # Direct download as fallback (no aggressive timeouts for large files)
        3.times do |attempt|
          @storage_client.with_http_client(with_timeouts: false) do |h|
            if mfile = @storage_client.download_segment(seg_id, @s3_segments, h)
              @segments[seg_id] = mfile
              return mfile
            end
          end
          break if @segments[seg_id]? # another fiber downloaded it
          @log.warn { "Failed to download segment #{seg_id} (attempt #{attempt + 1}/3)" }
          sleep (attempt + 1).seconds
        rescue ex : IO::Error | IO::TimeoutError
          @log.warn { "Error downloading segment #{seg_id} (attempt #{attempt + 1}/3): #{ex.message}" }
          sleep (attempt + 1).seconds
        end
        @segments[seg_id]?
      end

      # -- Overrides --

      def ensure_available(consumer : AMQP::StreamConsumer) : Nil
        seg = @segments[consumer.segment]?
        if seg.nil?
          download_segment(consumer.segment)
          seg = @segments[consumer.segment]?
        end
        # At EOF of current segment — prefetch the next one
        if seg && consumer.pos >= seg.size
          if next_seg = next_segment_id(consumer.segment)
            download_segment(next_seg) unless @segments[next_seg]?
          end
        end
      end

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
        @upload_queue.close
        compact_catalog
        @catalog_dirty = true # force upload on close
        upload_catalog_to_s3
        @catalog_io.try &.close
        @segment_cache.try &.close
        super
      end

      def delete
        super
        keys = Array(String).new(@s3_segments.size * 2 + 1)
        @s3_segments.each_value do |s3_seg|
          keys << s3_seg[:path]
          keys << @storage_client.meta_file_name(s3_seg[:path])
        end
        keys << @storage_client.catalog_file_name
        @storage_client.delete_objects(keys)
        @s3_segments.clear
        @catalog_io.try &.close
        @catalog_io = nil
      end

      private def next_segment(consumer) : MFile?
        result = super
        if result && (cache = @segment_cache)
          cache.current_read_segments[consumer.tag] = consumer.segment
        end
        result
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

      private def sort_segment_hashes!
        {% for ivar in ["@segment_msg_count", "@segment_first_offset", "@segment_first_ts", "@segment_last_ts"] %}
          begin
            sorted = {{ivar.id}}.to_a.sort_by!(&.[0])
            {{ivar.id}}.clear
            sorted.each { |k, v| {{ivar.id}}[k] = v }
          end
        {% end %}
      end

      # Download meta files and last segment from S3 so base init can process them
      private def prepare_local_files_from_s3
        return if @s3_segments.empty?

        # Download meta files only for segments not covered by the catalog
        catalog = @catalog_data
        @s3_segments.each do |seg_id, s3_seg|
          next unless s3_seg[:meta] # only download if meta exists in S3
          next if catalog && catalog.has_key?(seg_id)
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
        catalog = @catalog_data
        @s3_segments.each do |seg_id, s3_seg|
          already_counted = @segment_msg_count[seg_id]? && !@segment_msg_count[seg_id].zero?

          if catalog && (entry = catalog[seg_id]?)
            # Apply catalog data — always set offsets/timestamps (base init may
            # have set wrong first_offset for the last segment which was loaded
            # locally without a meta file). Only adjust counters for new segments.
            @segment_msg_count[seg_id] = entry[:msg_count]
            @segment_first_offset[seg_id] = entry[:first_offset]
            @segment_first_ts[seg_id] = entry[:first_ts]
            @segment_last_ts[seg_id] = entry[:last_ts]
            unless already_counted
              @size += entry[:msg_count]
              @bytesize += s3_seg[:size] - 4
            end
          elsif already_counted
            next
          elsif File.exists?(meta_path = File.join(@msg_dir, "meta.#{seg_id.to_s.rjust(10, '0')}"))
            read_s3_meta_file(seg_id, meta_path, s3_seg[:size])
          else
            # No meta file available, download segment and produce metadata
            @storage_client.with_http_client(with_timeouts: true) do |h|
              if mfile = @storage_client.download_segment(seg_id, @s3_segments, h)
                @segments[seg_id] = mfile
                mfile.pos = 4
                produce_metadata(seg_id, mfile)
                write_metadata_file(seg_id, mfile)
                upload_meta_to_s3(seg_id, mfile, h)
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

      private def upload_segment_to_s3(seg_id : UInt32, h : ::HTTP::Client)
        return if @s3_segments[seg_id]?
        return unless @segment_msg_count[seg_id]? && @segment_msg_count[seg_id] > 0

        mfile = @segments[seg_id]? || return
        s3_path = "/#{@storage_client.s3_path(mfile.path)}"
        @log.debug { "Uploading segment to S3: #{s3_path}" }
        etag = @storage_client.upload_file_to_s3(h, s3_path, mfile.to_slice)

        upload_meta_to_s3(seg_id, mfile, h)

        @s3_segments[seg_id] = {
          path: s3_path[1..],
          etag: etag,
          size: mfile.size,
          meta: true,
        }
        append_to_catalog(seg_id)
      end

      private def upload_meta_to_s3(seg_id : UInt32, mfile : MFile, h : ::HTTP::Client)
        meta_path = meta_file_name(mfile)
        if File.exists?(meta_path)
          s3_meta_path = "/#{@storage_client.s3_path(meta_path)}"
          @storage_client.upload_file_to_s3(h, s3_meta_path, File.open(meta_path, &.getb_to_end))
        end
      end

      private def upload_missing_segments_to_s3
        @segments.each_key do |seg_id|
          next if seg_id == @wfile_id
          next if @s3_segments[seg_id]?
          next unless @segment_msg_count[seg_id]? && @segment_msg_count[seg_id] > 0
          @upload_queue.send(seg_id)
        end
      end

      # -- Segment catalog methods --

      private def load_segment_catalog
        catalog_path = File.join(@msg_dir, "segments.catalog")

        # Download from S3 if not local
        unless File.exists?(catalog_path)
          @storage_client.with_http_client(with_timeouts: true) do |h|
            s3_path = @storage_client.catalog_file_name
            h.get("/#{s3_path}") do |response|
              if response.status_code == 200
                File.open(catalog_path, "w") do |f|
                  IO.copy response.body_io, f
                end
              end
            end
          rescue ex : IO::TimeoutError | IO::Error
            Log.warn { "Failed to download segment catalog: #{ex.message}" }
          end
        end

        # Parse catalog file
        catalog = Hash(UInt32, NamedTuple(msg_count: UInt32, first_offset: Int64, first_ts: Int64, last_ts: Int64)).new
        if File.exists?(catalog_path)
          file_size = File.size(catalog_path)
          remainder = file_size % CATALOG_RECORD_SIZE
          if remainder != 0
            Log.warn { "Segment catalog has #{remainder} trailing bytes, truncating" }
            File.open(catalog_path, "r+") do |f|
              f.truncate(file_size - remainder)
            end
          end
          File.open(catalog_path) do |f|
            while f.pos < f.size
              seg_id = f.read_bytes(UInt32)
              msg_count = f.read_bytes(UInt32)
              first_offset = f.read_bytes(Int64)
              first_ts = f.read_bytes(Int64)
              last_ts = f.read_bytes(Int64)
              catalog[seg_id] = {msg_count: msg_count, first_offset: first_offset, first_ts: first_ts, last_ts: last_ts}
            end
          rescue IO::EOFError
          end
          Log.info { "Loaded #{catalog.size} entries from segment catalog" }
        end

        @catalog_data = catalog
        @catalog_io = File.open(catalog_path, "a")
      end

      private def append_to_catalog(seg_id : UInt32)
        io = @catalog_io || return
        io.write_bytes(seg_id)
        io.write_bytes(@segment_msg_count[seg_id])
        io.write_bytes(@segment_first_offset[seg_id])
        io.write_bytes(@segment_first_ts[seg_id])
        io.write_bytes(@segment_last_ts[seg_id])
        io.flush
        @catalog_dirty = true
        if Time.instant - @last_catalog_upload >= CATALOG_UPLOAD_INTERVAL
          upload_catalog_to_s3
        end
      end

      private def upload_catalog_to_s3
        return unless @catalog_dirty
        catalog_path = File.join(@msg_dir, "segments.catalog")
        return unless File.exists?(catalog_path)
        s3_path = "/#{@storage_client.catalog_file_name}"
        @storage_client.upload_file_to_s3(s3_path, File.read(catalog_path).to_slice)
        @catalog_dirty = false
        @last_catalog_upload = Time.instant
      end

      private def compact_catalog
        catalog_path = File.join(@msg_dir, "segments.catalog")
        return unless File.exists?(catalog_path)
        record_count = File.size(catalog_path) // CATALOG_RECORD_SIZE
        return unless record_count > @s3_segments.size * 2

        @catalog_io.try &.close
        tmp_path = "#{catalog_path}.tmp"
        File.open(tmp_path, "w") do |f|
          @s3_segments.each_key do |seg_id|
            next unless @segment_msg_count[seg_id]?
            f.write_bytes(seg_id)
            f.write_bytes(@segment_msg_count[seg_id])
            f.write_bytes(@segment_first_offset[seg_id])
            f.write_bytes(@segment_first_ts[seg_id])
            f.write_bytes(@segment_last_ts[seg_id])
          end
        end
        File.rename(tmp_path, catalog_path)
        @catalog_io = File.open(catalog_path, "a")
      end
    end
  end
end
