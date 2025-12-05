require "./s3_storage_client"
require "./s3_message_store"
require "../../mfile"
require "../../rough_time"
require "../../config"

module LavinMQ::AMQP
  class S3SegmentCache
    @downloading_segments = Hash(UInt32, Int64).new
    @downloading_clients = Hash(UInt32, Array(::HTTP::Client)).new
    @log : Logger
    property current_read_segments = Hash(String, UInt32).new

    NUM_SEGMENTS_DOWNLOAD_AHEAD =   15
    MAX_RUNNING_FIBERS          =    8
    DOWNLOAD_TIMEOUT            = 2000

    def initialize(
      @storage_client : S3StorageClient,
      @s3_segments : Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)),
      @segments : Hash(UInt32, MFile),
      @msg_dir : String,
      metadata : ::Log::Metadata = ::Log::Metadata.empty,
    )
      @log = Logger.new(Log, metadata)
      delete_temp_files(@msg_dir)
    end

    def spawn_download_fibers
      spawn download_segments, name: "S3SegmentCache#download_segments"
      spawn monitor_downloads, name: "S3SegmentCache#monitor_downloads"
      spawn remove_local_segments, name: "S3SegmentCache#remove_local_segments"
    end

    def download_segments
      loop do
        sleep 50.milliseconds
        segments_to_download = segments_that_should_be_downloaded
        next if segments_to_download.empty?
        segments_to_download.reject! { |seg| @downloading_segments[seg]? || @segments[seg]? || !@s3_segments[seg]? }

        download_mutex = Mutex.new
        [MAX_RUNNING_FIBERS, @s3_segments.size].min.times do
          spawn do
            while segment = download_mutex.synchronize { segments_to_download.shift? }
              do_download(segment, @storage_client.http_client)
            end
          end
        end
      end
    end

    def do_download(segment, h)
      @downloading_segments[segment] = RoughTime.unix_ms
      path = File.join(Config.instance.data_dir, @s3_segments[segment][:path])
      File.delete(path) if File.exists?(path)
      @downloading_clients[segment] = [h]
      begin
        if mfile = @storage_client.download_segment(segment, @s3_segments, h)
          @segments[segment] = mfile
          @downloading_clients[segment]?.try &.delete(h)
        end
      rescue ex : IO::Error
        if cl = @downloading_clients.delete(segment)
          cl.each(&.close)
        end
        @log.debug { "Segment #{segment}: socket closed" }
      end
    end

    private def monitor_downloads
      loop do
        sleep 50.milliseconds
        next if @downloading_segments.empty?
        @log.debug { "Currently downloading segments: #{@downloading_segments}" }
        @downloading_segments.each do |seg_id, download_start_time|
          if @segments[seg_id]?
            @log.debug { "Segment #{seg_id} downloaded" }
            @downloading_segments.delete(seg_id)
            if clients = @downloading_clients.delete(seg_id)
              clients.each(&.close)
            end
            Dir.glob(File.join(Config.instance.data_dir, "msgs.#{seg_id.to_s.rjust(10, '0')}.tmp.*")).each do |file|
              File.delete(file)
            end
          elsif RoughTime.unix_ms - download_start_time > DOWNLOAD_TIMEOUT
            @log.debug { "Segment #{seg_id} download timed out, retrying" }
            @downloading_segments[seg_id] = RoughTime.unix_ms
            spawn retry_download_segment(seg_id)
          end
          Fiber.yield
        end
      end
    end

    private def retry_download_segment(seg_id)
      h = @storage_client.http_client
      @downloading_clients[seg_id] << h
      begin
        if mfile = @storage_client.download_segment(seg_id, @s3_segments, h)
          @segments[seg_id] = mfile
        end
      rescue ex : IO::Error
        @log.debug { "Segment #{seg_id}: socket closed during retry" }
        @downloading_segments[seg_id] = RoughTime.unix_ms - 2000
      end
    end

    private def segments_that_should_be_downloaded
      segments_per_consumer = if @current_read_segments.empty?
                                NUM_SEGMENTS_DOWNLOAD_AHEAD
                              else
                                Config.instance.streams_s3_storage_local_segments_per_stream // @current_read_segments.size
                              end

      @current_read_segments.flat_map do |_consumer, segment|
        (0...segments_per_consumer).map { |i| segment + i }
      end.to_set.to_a.sort_by do |segment|
        @current_read_segments.min_of { |_cid, consumer_segment| (segment - consumer_segment).abs }
      end
    end

    private def remove_local_segments
      max_local_segments = Config.instance.streams_s3_storage_local_segments_per_stream
      loop do
        sleep 5.seconds
        next if @segments.size <= max_local_segments
        @log.debug { "Removing local segments, current size: #{@segments.size}/#{max_local_segments}" }
        segments_to_remove = @segments.keys.reject do |seg_id|
          segments_that_should_be_downloaded.includes?(seg_id) || @current_read_segments.values.any? { |current_seg| current_seg == seg_id }
        end
        if @current_read_segments.empty?
          while @segments.size > max_local_segments && !segments_to_remove.empty?
            remove_local_segment(segments_to_remove.pop)
          end
        else
          segments_to_remove = remove_segments_before_readers(segments_to_remove)
          remove_furthest_segments(segments_to_remove)
        end
      end
    end

    private def remove_segments_before_readers(segments_to_remove)
      return segments_to_remove if @segments.size <= Config.instance.streams_s3_storage_local_segments_per_stream
      return segments_to_remove if segments_to_remove.empty?

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
        seg.delete if File.exists?(seg.path)
        seg.close
      end
    rescue ex : File::NotFoundError
      @log.debug { "File not found while trying to remove segment #{seg_id}" }
    end

    def delete_temp_files(msg_dir : String)
      Dir.each_child(msg_dir) do |f|
        if f.includes?(".tmp")
          File.delete(File.join(msg_dir, f))
        end
      end
    end
  end
end
