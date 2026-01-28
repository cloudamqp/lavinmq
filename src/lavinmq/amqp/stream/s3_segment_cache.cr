require "./s3_storage_client"
require "./s3_message_store"
require "../../mfile"
require "../../rough_time"
require "../../config"

module LavinMQ::AMQP
  class S3SegmentCache
    @log : Logger
    property current_read_segments = Hash(String, UInt32).new

    # Configuration
    NUM_SEGMENTS_DOWNLOAD_AHEAD =   15
    NUM_DOWNLOAD_WORKERS        =    8
    MAX_ATTEMPTS_PER_SEGMENT    =    3
    SLOW_DOWNLOAD_THRESHOLD_MS  = 2000
    COORDINATOR_INTERVAL        = 100.milliseconds
    CLEANUP_INTERVAL            = 5.seconds

    # Track in-flight download attempts
    private record DownloadAttempt, started_at : Int64, completed : Bool = false

    def initialize(
      @storage_client : S3StorageClient,
      @s3_segments : Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)),
      @segments : Hash(UInt32, MFile),
      @msg_dir : String,
      metadata : ::Log::Metadata = ::Log::Metadata.empty,
    )
      @log = Logger.new(Log, metadata)
      @mutex = Mutex.new
      @pending_downloads = Hash(UInt32, Array(DownloadAttempt)).new
      @download_queue = ::Channel(UInt32).new(256)
      @closed = false
      delete_temp_files(@msg_dir)
    end

    def spawn_download_fibers
      # Fixed pool of download workers
      NUM_DOWNLOAD_WORKERS.times do |i|
        spawn download_worker(i), name: "S3SegmentCache#worker-#{i}"
      end
      # Single coordinator handles all scheduling decisions
      spawn coordinator_loop, name: "S3SegmentCache#coordinator"
      # Cleanup runs less frequently
      spawn cleanup_loop, name: "S3SegmentCache#cleanup"
    end

    def close
      @closed = true
      @download_queue.close
    end

    # Fixed worker that processes download requests from the queue
    private def download_worker(worker_id : Int32)
      loop do
        seg_id = @download_queue.receive? || break # nil when channel closed

        # Skip if we already have this segment (another worker may have completed it)
        next if @mutex.synchronize { @segments[seg_id]? }

        @log.debug { "Worker #{worker_id}: downloading segment #{seg_id}" }

        # Each worker creates its own HTTP client per download
        # This ensures we potentially hit different S3 endpoints
        client = @storage_client.http_client(with_timeouts: true)

        begin
          if mfile = @storage_client.download_segment(seg_id, @s3_segments, client)
            @mutex.synchronize do
              # First one wins - don't overwrite if another worker finished first
              if @segments[seg_id]?
                @log.debug { "Worker #{worker_id}: segment #{seg_id} already downloaded by another worker" }
                mfile.delete
                mfile.close
              else
                @segments[seg_id] = mfile
                @log.debug { "Worker #{worker_id}: segment #{seg_id} downloaded successfully" }
              end
              # Mark all attempts for this segment as completed
              if attempts = @pending_downloads[seg_id]?
                @pending_downloads[seg_id] = attempts.map(&.copy_with(completed: true))
              end
            end
          end
        rescue ex : IO::Error | IO::TimeoutError
          @log.debug { "Worker #{worker_id}: segment #{seg_id} download failed: #{ex.message}" }
        ensure
          client.close
        end
      end
    rescue Channel::ClosedError
      # Expected when closing
    end

    # Single coordinator loop handles all scheduling decisions
    private def coordinator_loop
      until @closed
        sleep COORDINATOR_INTERVAL

        @mutex.synchronize do
          cleanup_completed_downloads
          schedule_downloads
        end
      end
    end

    # Remove tracking for segments that completed or exceeded max attempts
    private def cleanup_completed_downloads
      @pending_downloads.reject! do |seg_id, attempts|
        if @segments[seg_id]?
          # Download completed, clean up temp files
          cleanup_temp_files(seg_id)
          true
        elsif attempts.size >= MAX_ATTEMPTS_PER_SEGMENT && attempts.all? { |a| download_timed_out?(a) }
          # All attempts failed/timed out, give up for now
          # Will be retried on next coordinator loop if still wanted
          @log.warn { "Segment #{seg_id}: all #{MAX_ATTEMPTS_PER_SEGMENT} download attempts failed" }
          true
        else
          false
        end
      end
    end

    # Schedule downloads for segments we want but don't have
    private def schedule_downloads
      wanted = segments_that_should_be_downloaded

      wanted.each do |seg_id|
        next if @segments[seg_id]?        # Already have it locally
        next unless @s3_segments[seg_id]? # Must exist in S3

        attempts = @pending_downloads[seg_id]?

        if attempts.nil? || attempts.empty?
          # No attempts yet, start one
          queue_download(seg_id)
        elsif should_start_racing_download?(attempts)
          # Existing download is slow, start a racing attempt
          queue_download(seg_id)
        end
      end
    end

    private def should_start_racing_download?(attempts : Array(DownloadAttempt)) : Bool
      return false if attempts.size >= MAX_ATTEMPTS_PER_SEGMENT

      # Check if the most recent non-completed attempt is slow
      active_attempts = attempts.reject(&.completed)
      return true if active_attempts.empty? # All previous attempts completed/failed

      oldest_active = active_attempts.min_by(&.started_at)
      download_timed_out?(oldest_active)
    end

    private def download_timed_out?(attempt : DownloadAttempt) : Bool
      RoughTime.unix_ms - attempt.started_at > SLOW_DOWNLOAD_THRESHOLD_MS
    end

    private def queue_download(seg_id : UInt32)
      @pending_downloads[seg_id] ||= [] of DownloadAttempt
      @pending_downloads[seg_id] << DownloadAttempt.new(started_at: RoughTime.unix_ms)

      select
      when @download_queue.send(seg_id)
        @log.debug { "Queued download for segment #{seg_id} (attempt #{@pending_downloads[seg_id].size})" }
      else
        # Queue full, will retry next coordinator loop
        @log.debug { "Download queue full, deferring segment #{seg_id}" }
        @pending_downloads[seg_id].pop # Remove the attempt we just added
      end
    end

    private def cleanup_temp_files(seg_id : UInt32)
      pattern = File.join(@msg_dir, "msgs.#{seg_id.to_s.rjust(10, '0')}.tmp*")
      Dir.glob(pattern).each do |file|
        File.delete(file) rescue nil
      end
    end

    # Calculate which segments should be cached locally based on consumer positions
    private def segments_that_should_be_downloaded : Array(UInt32)
      segments_per_consumer = if @current_read_segments.empty?
                                NUM_SEGMENTS_DOWNLOAD_AHEAD
                              else
                                Config.instance.streams_s3_storage_local_segments_per_stream // @current_read_segments.size
                              end

      # For each consumer, want segments from their position forward
      @current_read_segments.flat_map do |_consumer, segment|
        (0...segments_per_consumer).map { |i| segment + i }
      end.to_set.to_a.sort_by do |segment|
        # Prioritize segments closest to any consumer
        @current_read_segments.min_of { |_cid, consumer_segment| (segment - consumer_segment).abs }
      end
    end

    # Cleanup loop removes segments we no longer need locally
    private def cleanup_loop
      max_local_segments = Config.instance.streams_s3_storage_local_segments_per_stream

      until @closed
        sleep CLEANUP_INTERVAL

        @mutex.synchronize do
          next if @segments.size <= max_local_segments

          @log.debug { "Cleanup: #{@segments.size}/#{max_local_segments} local segments" }

          wanted = segments_that_should_be_downloaded.to_set
          currently_reading = @current_read_segments.values.to_set

          # Find segments safe to remove (not wanted and not currently being read)
          removable = @segments.keys.reject do |seg_id|
            wanted.includes?(seg_id) || currently_reading.includes?(seg_id)
          end

          if @current_read_segments.empty?
            # No consumers, just keep under limit
            remove_until_under_limit(removable, max_local_segments)
          else
            # First remove segments behind all readers
            min_reader_segment = @current_read_segments.values.min
            behind_readers = removable.select { |seg_id| seg_id < min_reader_segment }
            remove_segments(behind_readers, max_local_segments)

            # Then remove furthest from any reader
            if @segments.size > max_local_segments
              remaining = removable - behind_readers
              remove_furthest_from_readers(remaining, max_local_segments)
            end
          end
        end
      end
    end

    private def remove_until_under_limit(removable : Array(UInt32), max_local_segments : Int32)
      removable.each do |seg_id|
        break if @segments.size <= max_local_segments
        remove_local_segment(seg_id)
      end
    end

    private def remove_segments(segments : Array(UInt32), max_local_segments : Int32)
      segments.each do |seg_id|
        break if @segments.size <= max_local_segments
        remove_local_segment(seg_id)
      end
    end

    private def remove_furthest_from_readers(removable, max_local_segments : Int32)
      # Sort by distance from nearest reader (furthest first)
      sorted = removable.to_a.sort_by do |seg_id|
        distance = @current_read_segments.values.min_of { |read_seg| (seg_id.to_i64 - read_seg.to_i64).abs }
        -distance
      end

      sorted.each do |seg_id|
        break if @segments.size <= max_local_segments
        remove_local_segment(seg_id)
      end
    end

    private def remove_local_segment(seg_id : UInt32)
      @log.debug { "Removing local segment: #{seg_id}" }
      if seg = @segments.delete(seg_id)
        seg.delete if File.exists?(seg.path)
        seg.close
      end
    rescue ex : File::NotFoundError
      @log.debug { "File not found while removing segment #{seg_id}" }
    end

    def delete_temp_files(msg_dir : String)
      Dir.each_child(msg_dir) do |f|
        if f.includes?(".tmp")
          File.delete(File.join(msg_dir, f)) rescue nil
        end
      end
    end
  end
end
