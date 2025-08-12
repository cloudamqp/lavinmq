require "./stream_queue"
require "../stream_consumer"
require "../../mfile"
require "awscr-signer"
require "http/client"
require "xml"
require "../../schema"

module LavinMQ::AMQP
  class StreamQueue < DurableQueue
    class StreamS3MessageStore < StreamQueueMessageStore
      @s3_segments = Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)).new
      property current_read_segments = Hash(String, UInt32).new
      @downloading_segments = Hash(UInt32, Int64).new
      @downloading_clients = Hash(UInt32, Array(::HTTP::Client)).new
      @s3_signer : Awscr::Signer::Signers::V4?
      NUM_SEGMENTS_DOWNLOAD_AHEAD =   15             # Number of segments to download ahead of current consumers
      MAX_RUNNING_FIBERS          =    8             # Maximum number of concurrent downloads
      DOWNLOAD_TIMEOUT            = 2000             # Timeout for downloading a segment
      HTTP_CONNECT_TIMEOUT        = 200.milliseconds # Timeout for HTTP connection. Low value to quickly retry.
      HTTP_READ_TIMEOUT           = 500.milliseconds # Timeout for reading from HTTP connection. Low value to quickly retry.

      def initialize(@msg_dir : String, @replicator : Clustering::Replicator?,
                     durable : Bool = true, metadata : ::Log::Metadata = ::Log::Metadata.empty)
        @log = Logger.new(Log, metadata)
        @durable = durable
        @acks = Hash(UInt32, MFile).new
        @consumer_offsets = MFile.new(File.join(@msg_dir, "consumer_offsets"), Config.instance.segment_size)
        @last_offset = 0_i64

        s3_segments_from_bucket
        delete_temp_files

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
        spawn download_segments, name: "StreamS3MessageStore#download_segments" # asynchronously download segments ahead of current consumers
        spawn monitor_downloads, name: "StreamS3MessageStore#monitor_downloads" # monitor downloads and retry if needed
        spawn remove_local_segments, name: "StreamS3MessageStore#remove_local_segments"
      end

      # Get a list of segments from s3
      private def s3_segments_from_bucket(retries = 5)
        prefix = @msg_dir[Config.instance.data_dir.bytesize + 1..] + "/"
        cont_token = ""
        loop do
          path = "?delimiter=%2F&encoding-type=url&list-type=2&prefix=#{prefix}&max-keys=1000"
          path += "&continuation-token=#{URI.encode_path(cont_token)}" unless cont_token.empty?
          h = http(URI.parse "https://#{Config.instance.streams_s3_storage_bucket}.#{Config.instance.streams_s3_storage_endpoint}")
          response = h.get(path)
          cont_token = list_of_files_from_xml(XML.parse(response.body))
          break unless cont_token # no more pages to process
        end
      rescue ex : IO::TimeoutError # retry
        @log.error { "Timeout while downloading file list, retrying..." }
        s3_segments_from_bucket(retries - 1) if retries > 0
      end

      # Parse the XML response from S3
      def list_of_files_from_xml(body)
        list_bucket_results = body.first_element_child
        return unless list_bucket_results

        contents_elements = list_bucket_results.children.select(&.element?).select(&.name.==("Contents"))
        contents_elements.each { |content| parse_xml_element(content) }
        @log.info { "Found #{@s3_segments.size} segments in S3" }

        continuation_token(list_bucket_results)
      end

      private def continuation_token(list_bucket_results)
        is_truncated = list_bucket_results.children.select(&.element?).find { |c| (c.name.==("IsTruncated")) }.try &.children.first?.try &.content
        if is_truncated == "true"
          continuation_token = list_bucket_results.children.select(&.element?).find { |c| (c.name.==("NextContinuationToken")) }.try &.children.first?.try &.content
          return continuation_token
        end
        nil
      end

      private def parse_xml_element(content)
        path = etag = ""
        id = 0_u32
        size = 0_i64

        content.children.select(&.element?).each do |element|
          case element.name
          when "Key"
            path = element.content
            return nil unless path.includes?("msgs.")
            if path.includes?(".meta")
              update_s3_segment_list(path[-15..-6].to_u32, "", "", 0_i64, true)
              return
            end
            id = path[-10..].to_u32
          when "ETag"
            if element.content.starts_with?('"')
              etag = element.content[1..-2] # Remove quotes from ETag
            else
              etag = element.content
            end
          when "Size"
            size = element.content.to_i64
          end
        end
        update_s3_segment_list(id, path, etag, size, false)
      end

      private def update_s3_segment_list(seg_id : UInt32, path : String = "", etag : String = "", size : Int64 = 0_i64, meta : Bool = false)
        s3_seg = @s3_segments[seg_id]? || {path: path, etag: etag, size: size, meta: meta}
        path = s3_seg[:path] if path == ""
        etag = s3_seg[:etag] if etag == ""
        size = s3_seg[:size] if size == 0_i64
        meta = s3_seg[:meta] if meta == false

        @s3_segments[seg_id] = {path: path, etag: etag, size: size, meta: meta}
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
          if @segment_msg_count[seg].zero? # only load segment if stats not already read from .meta file
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
        wg = WaitGroup.new(@s3_segments.size)
        segments = Array(Tuple(UInt32, MFile)).new(@s3_segments.size)
        counter = 0
        running_fibers = 0
        @s3_segments.each do |seg, s3file|
          while running_fibers >= MAX_RUNNING_FIBERS
            Fiber.yield
          end
          running_fibers += 1
          spawn do
            path = File.join(Config.instance.data_dir, s3file[:path])
            if File.exists?("#{path}.meta")
              read_metadata_file(seg, path, s3file[:size])
              unless s3file[:meta] # upload to s3 unless it exists there
                Log.info { "Uploading metadata file for segment #{seg} to S3" }
                slice = Bytes.new(20)
                File.open("#{path}.meta", &.read_fully(slice))
                upload_file_to_s3("/#{path[Config.instance.data_dir.bytesize + 1..]}.meta", slice)
              end
            elsif meta_file = download_meta_file(seg, http_client)
              read_metadata_file_from_s3(seg, meta_file)
            end
            file = verify_local_file?(s3file, seg)

            # download segment from s3 if meta file was not found and local file is not valid
            # always download last segment unless it exists locally
            if @segment_msg_count[seg].zero? || seg == @s3_segments.last_key
              file ||= download_segment(seg, http_client)
              if mfile = file
                @replicator.try &.register_file mfile
                segments << {seg, mfile}
                mfile.pos = 4
                produce_metadata(seg, mfile)
                write_metadata_file(seg, mfile)
                slice = Bytes.new(20)
                File.open("#{path}.meta", &.read_fully(slice))
                upload_file_to_s3("/#{path[Config.instance.data_dir.bytesize + 1..]}.meta", slice)
              else
                @log.error { "Failed to load segment #{path}" }
              end
            end
            if is_long_queue
              @log.info { "Loaded #{counter}/#{@s3_segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
            else
              @log.debug { "Loaded #{counter}/#{@s3_segments.size} segments, #{@size} messages" } if (counter &+= 1) % 128 == 0
            end
            Fiber.yield
            running_fibers -= 1
            wg.done
          end
        end
        wg.wait
        segments
      end

      private def read_metadata_file(seg, path, bytesize)
        return unless File.exists?("#{path}.meta")
        File.open("#{path}.meta") do |file|
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

      # Wait for download of the next segment if needed
      private def next_segment(consumer) : MFile?
        consumer.segment += 1
        @current_read_segments[consumer.tag] = consumer.segment
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

      def download_segments
        loop do
          sleep 50.milliseconds
          segments_to_download = segments_that_should_be_downloaded
          next if segments_to_download.empty?
          segments_to_download.reject! { |seg| @downloading_segments[seg]? || @segments[seg]? || !@s3_segments[seg]? }

          segments_to_download.each do |segment|
            while @downloading_segments.size >= MAX_RUNNING_FIBERS
              sleep 10.milliseconds
            end

            @downloading_segments[segment] = RoughTime.unix_ms
            spawn do
              path = File.join(Config.instance.data_dir, @s3_segments[segment][:path])
              File.delete(path) if File.exists?(path) # delete old file if exists
              h = http_client
              @downloading_clients[segment] = [h]
              begin
                if mfile = download_segment(segment, h)
                  @segments[segment] = mfile
                end
              rescue ex : IO::Error
                @downloading_clients.delete(segment)
                @log.error { "Segment #{segment}: socket closed" }
              end
            end
            Fiber.yield
          end
        end
      end

      # TODO: Cancel downloads if consumer disconnects?
      private def monitor_downloads
        loop do
          sleep 50.milliseconds
          next if @downloading_segments.empty?
          @log.debug { "Currently downloading segments: #{@downloading_segments}" }
          @downloading_segments.each do |seg_id, download_start_time|
            if @segments[seg_id]? # Segment downloaded
              @log.debug { "Segment #{seg_id} downloaded" }
              @downloading_segments.delete(seg_id)
              if clients = @downloading_clients.delete(seg_id)
                clients.each(&.close) # Close any client still trying to download completed segment
              end
            elsif RoughTime.unix_ms - download_start_time > DOWNLOAD_TIMEOUT
              @log.debug { "Segment #{seg_id} download timed out, retrying" }
              @downloading_segments[seg_id] = RoughTime.unix_ms
              spawn do
                h = http_client
                @downloading_clients[seg_id] << h
                begin
                  if mfile = download_segment(seg_id, h)
                    @segments[seg_id] = mfile
                  end
                rescue ex : IO::Error
                  @log.debug { "Segment #{seg_id}: socket closed during retry" }
                  @downloading_segments[seg_id] = RoughTime.unix_ms - 2000 # Reset the start time to retry again
                end
              end
            end
            Fiber.yield
          end
        end
      end

      # Get a list of segments that should be downloaded based on current consumers
      private def segments_that_should_be_downloaded
        segments_per_consumer = if @current_read_segments.empty?
                                  NUM_SEGMENTS_DOWNLOAD_AHEAD
                                else
                                  Config.instance.streams_s3_storage_local_segments_per_stream // @current_read_segments.size
                                end
        # Create a list of segments that should be downloaded (current read segments + nr of ahead segments)
        # And sort them by distance to current read segments, so that the closest segments are downloaded first
        @current_read_segments.flat_map do |_consumer, segment|
          (0...segments_per_consumer).map { |i| segment + i }
        end.to_set.to_a.sort_by do |segment|
          @current_read_segments.min_of { |_cid, consumer_segment| (segment - consumer_segment).abs }
        end
      end

      private def download_segment(segment_id : UInt32, h = http_client) : MFile?
        @log.debug { "Downloading segment: #{segment_id}" }
        return unless @s3_segments[segment_id]?
        s3file_path = @s3_segments[segment_id][:path]
        path = File.join(Config.instance.data_dir, s3file_path)

        h.get("/#{s3file_path}") do |response|
          bytesize = response.headers["Content-Length"].to_i32
          rfile = MFile.new(temp_path(path), bytesize) # Create a temporary file
          IO.copy response.body_io, rfile
          if File.exists?(path)
            rfile.delete # Delete temp file if segment already exists
            return
          else
            rfile.rename(path)
            @log.debug { "Downloaded segment: #{segment_id}" }
            return rfile
          end
        end
      rescue ex : IO::TimeoutError
        @log.warn { "Timeout while downloading segment #{segment_id}, retrying" }
        download_segment(segment_id, h) # Retry on timeout
      end

      private def download_meta_file(segment_id : UInt32, h = http_client) : MFile?
        @log.debug { "Downloading meta for segment: #{segment_id}" }
        return unless @s3_segments[segment_id]?
        s3_meta_path = "#{@s3_segments[segment_id][:path]}.meta"
        path = File.join(Config.instance.data_dir, s3_meta_path)

        h.get("/#{s3_meta_path}") do |response|
          if response.status_code != 200
            @log.error { "Failed to download meta for segment #{segment_id}, status: #{response.status_code}" }
            return nil
          end
          bytesize = response.headers["Content-Length"].to_i32
          rfile = MFile.new(path, bytesize)
          IO.copy response.body_io, rfile
          return rfile
        end
      rescue ex : IO::TimeoutError
        @log.warn { "Timeout while downloading meta for segment #{segment_id}, retrying" }
        download_meta_file(segment_id, h) # Retry on timeout
      end

      private def temp_path(path)
        temp_path = "#{path}.tmp"
        i = 0
        while File.exists?(temp_path)
          temp_path = "#{path}.tmp.#{i}"
          i += 1
        end
        temp_path
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
        @offset_index[@segments.last_key] = @last_offset + 1
        @timestamp_index[@segments.last_key] = RoughTime.unix_ms

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

      private def upload_segment_to_s3(segment, seg_id, retries = 3)
        return if @s3_segments[seg_id]?
        return unless @segment_msg_count[seg_id] > 0 # don't upload empty segments
        @log.debug { "Uploading file to s3: /#{segment.path[Config.instance.data_dir.bytesize + 1..]}" }
        path = "/#{segment.path[Config.instance.data_dir.bytesize + 1..]}"
        etag = upload_file_to_s3(path, segment.to_slice)
        meta_path = "#{segment.path}.meta"
        if File.exists?(meta_path)
          upload_file_to_s3("#{path}.meta", File.open(meta_path, &.getb_to_end)) # meta file
        end

        @s3_segments[seg_id] = {
          path: path[1..],
          etag: etag,
          size: segment.size,
          meta: true,
        }
      end

      private def upload_file_to_s3(path, slice, retries = 3)
        h = http(URI.parse "https://#{Config.instance.streams_s3_storage_bucket}.#{Config.instance.streams_s3_storage_endpoint}")
        response = h.put(path, body: slice)
        if response.status_code != 200
          if (retries -= 1) <= 0
            raise Exception.new("Failed to upload file #{path} to S3 after multiple retries")
          end
          @log.warn { "Failed to upload file #{path} to S3, retrying" }
          upload_file_to_s3(path, slice, retries)
        end
        response.headers["ETag"]
      end

      # Remove local segments if needed.
      # Try to predict what segments will be needed by current consumers and keep them.
      private def remove_local_segments
        max_local_segments = Config.instance.streams_s3_storage_local_segments_per_stream
        loop do
          sleep 5.seconds
          next if @segments.size <= max_local_segments
          @log.debug { "Removing local segments, current size: #{@segments.size}/#{max_local_segments}" }
          segments_to_remove = @segments.keys.reject do |seg_id|
            segments_that_should_be_downloaded.includes?(seg_id) || seg_id == @wfile_id
          end
          if @current_read_segments.empty? # If there are no current read segments, remove the last segments
            while @segments.size > max_local_segments && !segments_to_remove.empty?
              remove_local_segment(segments_to_remove.pop)
            end
          else # Remove segments before current read segments or furthest from readers
            segments_to_remove = remove_segments_before_readers(segments_to_remove)
            remove_furthest_segments(segments_to_remove)
          end
        end
      end

      private def remove_segments_before_readers(segments_to_remove)
        return segments_to_remove if @segments.size <= Config.instance.streams_s3_storage_local_segments_per_stream
        return if segments_to_remove.empty?
        segments_before_any_read_segments = segments_to_remove.select { |seg_id| seg_id < @current_read_segments.values.min }
        segments_before_any_read_segments.size.times do |i|
          break if @segments.size <= Config.instance.streams_s3_storage_local_segments_per_stream
          remove_local_segment(segments_before_any_read_segments[i])
          segments_to_remove.delete(segments_before_any_read_segments[i])
        end
        segments_to_remove
      end

      private def remove_furthest_segments(segments_to_remove)
        return if @segments.size <= Config.instance.streams_s3_storage_local_segments_per_stream
        if segments = segments_to_remove
          while @segments.size > Config.instance.streams_s3_storage_local_segments_per_stream
            furthest_segment = segments.max_by do |seg_id|
              @current_read_segments.values.min_of { |read_seg| (read_seg - seg_id).abs }
            end
            remove_local_segment(furthest_segment)
            segments.delete(furthest_segment)
          end
        end
      end

      private def remove_local_segment(seg_id)
        @log.debug { "Removing segment: #{seg_id} from local storage" }
        if seg = @segments.delete(seg_id)
          delete_file(seg)
          seg.close
        end
      rescue ex : File::NotFoundError
        @log.debug { "File not found while trying to remove segment #{seg_id}" }
      end

      private def delete_temp_files
        Dir.each_child(@msg_dir) do |f|
          if f.includes?(".tmp")
            File.delete(File.join(@msg_dir, f))
          end
        end
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
          delete_from_s3(s3_seg)
          if mfile = @segments.delete(seg_id)
            delete_file(mfile)
          end
          true
        end
      end

      private def delete_from_s3(s3_seg)
        h = http(URI.parse "https://#{Config.instance.streams_s3_storage_bucket}.#{Config.instance.streams_s3_storage_endpoint}")
        delete_from_s3(h, s3_seg[:path])
        delete_from_s3(h, "#{s3_seg[:path]}.meta")
      end

      private def delete_from_s3(h : ::HTTP::Client, path : String)
        response = h.delete("/#{path}")
        if response.status_code != 204
          @log.error { "Failed to delete #{path} from S3, status: #{response.status_code}" }
        else
          @log.debug { "Deleted #{path} from S3" }
        end
      end

      def delete
        super
        @s3_segments.reject! do |_seg_id, s3_seg|
          delete_from_s3(s3_seg)
        end
      end

      # Download segment if needed
      private def find_offset_in_segments(offset : Int | Time) : Tuple(Int64, UInt32, UInt32)
        segment = offset_index_lookup(offset)
        unless @segments[segment]?
          if mfile = download_segment(segment)
            @segments[segment] = mfile
          else
            raise "Segment #{segment} not found in S3 and could not be downloaded"
          end
        end
        super
      end

      private def http_client
        h = http(URI.parse "https://#{Config.instance.streams_s3_storage_bucket}.#{Config.instance.streams_s3_storage_endpoint}")
        h.connect_timeout = HTTP_CONNECT_TIMEOUT
        h.read_timeout = HTTP_READ_TIMEOUT
        h
      end

      def http(uri : URI)
        h = ::HTTP::Client.new(uri)
        h.before_request do |request|
          if signer = s3_signer
            signer.sign(request)
          else
            raise "No S3 signer found"
          end
        end
        h
      end

      def s3_signer : Awscr::Signer::Signers::V4?
        return @s3_signer if @s3_signer
        if (region = Config.instance.streams_s3_storage_region) &&
           (access_key = Config.instance.streams_s3_storage_access_key_id) &&
           (secret_key = Config.instance.streams_s3_storage_secret_access_key)
          @s3_signer = Awscr::Signer::Signers::V4.new("s3", region, access_key, secret_key)
        else
          Log.fatal { "S3 storage for streams is enabled, but region or access key is not set" }
          abort "S3 storage for streams is enabled, but region or access key is not set"
        end
        @s3_signer
      end
    end
  end
end
