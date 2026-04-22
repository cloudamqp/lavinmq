require "./blob_storage_client"
require "../../mfile"
require "../../rough_time"
require "../../config"

module LavinMQ::AMQP
  class BlobSegmentCache
    @log : Logger
    @idle_since : Time::Instant? = nil
    property current_read_segments = Hash(String, UInt32).new

    NUM_DOWNLOAD_WORKERS  = 8
    COORDINATOR_INTERVAL  = 100.milliseconds
    CLEANUP_INTERVAL      = 5.seconds
    IDLE_SHUTDOWN_TIMEOUT = 30.seconds

    def initialize(
      @storage_client : BlobStorageClient,
      @remote_segments : Hash(UInt32, NamedTuple(path: String, etag: String, size: Int64, meta: Bool)),
      @segments : Hash(UInt32, MFile),
      @msg_dir : String,
      metadata : ::Log::Metadata = ::Log::Metadata.empty,
    )
      @log = Logger.new(Log, metadata)
      @segments_mutex = Mutex.new
      @pending_mutex = Mutex.new
      @pending = Set(UInt32).new
      @download_queue = ::Channel(UInt32).new(256)
      @fibers_running = Atomic(Bool).new(false)
      @closed = false
      @running_loops = Atomic(Int32).new(0)
      @cleanup_signal = ::Channel(Nil).new(1)
      delete_temp_files(@msg_dir)
    end

    # Start background fibers for prefetching and cleanup.
    # Called when a consumer is added. No-op if already running.
    def ensure_fibers_running
      return if @closed
      return unless @fibers_running.compare_and_set(false, true)[1]
      # Recreate channels if they were closed by a previous shutdown
      if @download_queue.closed?
        @download_queue = ::Channel(UInt32).new(256)
        @cleanup_signal = ::Channel(Nil).new(1)
      end
      @running_loops.set(2) # coordinator + cleanup
      NUM_DOWNLOAD_WORKERS.times do |i|
        spawn download_worker(i), name: "BlobSegmentCache#worker-#{i}"
      end
      spawn coordinator_loop, name: "BlobSegmentCache#coordinator"
      spawn cleanup_loop, name: "BlobSegmentCache#cleanup"
    end

    # Signal that a consumer was removed. Triggers an immediate cleanup pass.
    def notify_consumer_removed
      @idle_since = Time.instant if @current_read_segments.empty?
      # Signal cleanup loop to run immediately
      select
      when @cleanup_signal.send(nil)
      else
      end
    end

    # Signal that a segment was uploaded. Triggers an immediate cleanup pass.
    def notify_upload_complete
      select
      when @cleanup_signal.send(nil)
      else
      end
    end

    # Called when a consumer connects. Clears idle timer.
    def notify_consumer_added
      @idle_since = nil
    end

    def close
      @closed = true
      @download_queue.close
      @cleanup_signal.close
    end

    private def download_worker(worker_id : Int32)
      client = @storage_client.http_client(with_timeouts: true)
      loop do
        seg_id = @download_queue.receive? || break
        next if @segments[seg_id]?

        @log.debug { "Worker #{worker_id}: downloading segment #{seg_id}" }
        begin
          if mfile = @storage_client.download_segment(seg_id, @remote_segments, client)
            @segments_mutex.synchronize do
              if @segments[seg_id]?
                # Another worker finished first, discard our copy
                @log.debug { "Worker #{worker_id}: segment #{seg_id} already downloaded" }
                mfile.delete
                mfile.close
              else
                @segments[seg_id] = mfile
                @log.debug { "Worker #{worker_id}: segment #{seg_id} downloaded" }
              end
            end
            @pending_mutex.synchronize { @pending.delete(seg_id) }
          else
            @pending_mutex.synchronize { @pending.delete(seg_id) }
          end
        rescue ex : IO::Error | IO::TimeoutError
          @log.debug { "Worker #{worker_id}: segment #{seg_id} download failed: #{ex.message}" }
          @pending_mutex.synchronize { @pending.delete(seg_id) }
          # Reopen connection on error
          client.close rescue nil
          client = @storage_client.http_client(with_timeouts: true)
        end
      end
    rescue ::Channel::ClosedError
    ensure
      client.try &.close
    end

    private def coordinator_loop
      until @closed
        sleep COORDINATOR_INTERVAL
        break if @closed

        if @current_read_segments.empty?
          if idle_timeout_reached?
            @log.debug { "Coordinator: idle timeout, shutting down" }
            break
          end
          next
        end

        schedule_downloads
      end
      mark_fibers_stopped
    end

    private def schedule_downloads
      wanted = segments_to_prefetch

      wanted.each do |seg_id|
        next if @segments[seg_id]?            # Already local
        next unless @remote_segments[seg_id]? # Must exist in remote storage

        should_queue = @pending_mutex.synchronize do
          next false if @pending.includes?(seg_id)
          @pending.add(seg_id)
          true
        end
        next unless should_queue

        select
        when @download_queue.send(seg_id)
          @log.debug { "Queued prefetch for segment #{seg_id}" }
        else
          @pending_mutex.synchronize { @pending.delete(seg_id) }
        end
      end
    end

    # Segments to prefetch based on consumer positions.
    # Returns segments sorted by proximity to the nearest consumer.
    private def segments_to_prefetch : Array(UInt32)
      readers = @current_read_segments
      return [] of UInt32 if readers.empty?

      # Ensure at least 3 segments per consumer so prefetching is useful even
      # when many consumers share a small local_segments_per_stream budget
      budget = Math.max(3, Config.instance.blob_storage_local_segments_per_stream // readers.size)

      readers.flat_map do |_consumer, segment|
        (0...budget).map { |i| segment + i }
      end.to_set.to_a.sort_by do |segment|
        readers.min_of { |_cid, consumer_seg| (segment.to_i64 - consumer_seg.to_i64).abs }
      end
    end

    # Evict local segment copies that are no longer needed.
    # Keeps the total under the configured max, prioritizing segments
    # that consumers are likely to need.
    private def cleanup_loop
      until @closed
        # Wait for either the cleanup interval or an explicit signal
        # (e.g. from remove_consumer triggering immediate cleanup)
        select
        when @cleanup_signal.receive
        when timeout(CLEANUP_INTERVAL)
        end
        break if @closed

        run_cleanup

        if @current_read_segments.empty? && idle_timeout_reached?
          run_cleanup # Final pass before shutting down
          @log.debug { "Cleanup: idle timeout, shutting down" }
          break
        end
      end
      mark_fibers_stopped
    rescue ::Channel::ClosedError
      mark_fibers_stopped
    end

    private def run_cleanup
      max_local = Config.instance.blob_storage_local_segments_per_stream
      return if @segments.size <= max_local

      wanted = segments_to_prefetch.to_set
      currently_reading = @current_read_segments.values.to_set

      # Segments safe to remove: not wanted by prefetch, not actively being read,
      # and re-downloadable from remote storage (never remove the write segment or local-only segments)
      removable = @segments.keys.reject do |seg_id|
        wanted.includes?(seg_id) || currently_reading.includes?(seg_id) || !@remote_segments[seg_id]?
      end

      if @current_read_segments.empty?
        # No consumers — just remove until under the limit
        removable.each do |seg_id|
          break if @segments.size <= max_local
          remove_local_segment(seg_id)
        end
      else
        # The lowest segment ID any consumer is currently reading
        min_reader_seg = @current_read_segments.values.min

        # Sort so we remove the least useful segments first:
        # - Segments behind all readers (already consumed) are removed first
        # - Among remaining, furthest from any reader are removed first
        sorted = removable.sort_by do |seg_id|
          dist = @current_read_segments.values.min_of { |rs| (seg_id.to_i64 - rs.to_i64).abs }
          seg_id < min_reader_seg ? -dist : dist
        end

        sorted.each do |seg_id|
          break if @segments.size <= max_local
          remove_local_segment(seg_id)
        end
      end
    end

    private def idle_timeout_reached? : Bool
      if idle_since = @idle_since
        Time.instant >= idle_since + IDLE_SHUTDOWN_TIMEOUT
      else
        false
      end
    end

    # Called when coordinator or cleanup loop exits.
    # When both have exited, close the download queue so workers exit too.
    private def mark_fibers_stopped
      if @running_loops.sub(1) == 1 # was 1, now 0 — we're the last loop
        @download_queue.close
        @cleanup_signal.close
        @fibers_running.set(false)
        @log.debug { "All background fibers stopped" }
      end
    end

    private def remove_local_segment(seg_id : UInt32)
      @log.debug { "Removing local segment: #{seg_id}" }
      seg = @segments_mutex.synchronize { @segments.delete(seg_id) }
      if seg
        seg.delete if File.exists?(seg.path)
        seg.close
      end
    rescue ex : File::NotFoundError
      @log.debug { "File not found while removing segment #{seg_id}" }
    end

    private def delete_temp_files(msg_dir : String)
      Dir.each_child(msg_dir) do |f|
        if f.includes?(".tmp")
          File.delete(File.join(msg_dir, f)) rescue nil
        end
      end
    end
  end
end
