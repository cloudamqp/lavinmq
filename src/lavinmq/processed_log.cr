require "file_utils"
require "./bool_channel"

module LavinMQ
  # Append-only log of per-ack metadata for one queue. Receives records from
  # the ack hot path through a bounded channel, flushes them to per-queue
  # segment files on a background fiber, and exposes query/summary reads.
  #
  # The log is observability state, not protocol state — it is leader-only,
  # not replicated, and writes are not fsync'd. Retention is time-based.
  class ProcessedLog
    MAGIC                    = "PRLG".to_slice
    VERSION                  = 1_u32
    HEADER_SIZE              = 4 + 4 + 8 + 8 # magic + version + first_ts + last_ts
    FILE_PREFIX              = "processed."
    FLUSH_INTERVAL           = 1.second
    RETENTION_SWEEP_INTERVAL = 60.seconds
    BATCH_MAX                = 256

    Log = LavinMQ::Log.for "processed_log"

    record Record,
      ack_ts_ms : Int64,
      latency_ms : Int64,
      payload_size : UInt32,
      redelivery_count : UInt32,
      exchange : String,
      routing_key : String,
      consumer_tag : String

    record Summary,
      count : UInt64,
      latency_p50 : Int64,
      latency_p95 : Int64,
      latency_p99 : Int64,
      latency_avg : Int64,
      redeliveries_avg : Float64,
      redeliveries_max : UInt32,
      redeliveries_histogram : Array(UInt64), # buckets: 0, 1-3, 4-7, 8+
      payload_size_avg : UInt64,
      dropped : UInt64

    @write_file : File?
    @write_segment_id : UInt64 = 0
    @write_size : Int64 = 0
    @write_first_ts : Int64 = 0
    @last_retention_sweep : Time::Instant

    def initialize(@data_dir : String,
                   @retention_ms : Int64,
                   @segment_size : Int64,
                   buffer_capacity : Int32)
      @ch = Channel(Record).new(buffer_capacity)
      @dropped = Atomic(UInt64).new(0_u64)
      @closed = Atomic(Bool).new(false)
      @stopped = BoolChannel.new(false)
      @last_retention_sweep = Time.instant
      @flusher_done = ::Channel(Nil).new
      open_or_create_write_segment
      spawn run_flusher, name: "ProcessedLog flusher #{@data_dir}"
    end

    # Non-blocking. Dropped (counted) when buffer is full so the ack path
    # is never blocked by observability.
    def record(rec : Record) : Nil
      return if @closed.get(:relaxed)
      select
      when @ch.send(rec)
        # queued
      else
        @dropped.add(1_u64, :relaxed)
      end
    end

    def query(from_ts : Int64, to_ts : Int64, offset : Int32, limit : Int32) : Array(Record)
      results = Array(Record).new
      skipped = 0
      each_segment_newest_first do |path|
        next unless segment_overlaps?(path, from_ts, to_ts)
        records_in_segment = [] of Record
        scan_segment(path) do |rec|
          next if rec.ack_ts_ms < from_ts
          next if rec.ack_ts_ms > to_ts
          records_in_segment << rec
        end
        records_in_segment.reverse_each do |rec|
          if skipped < offset
            skipped += 1
            next
          end
          results << rec
          return results if results.size >= limit
        end
      end
      results
    end

    def summary(from_ts : Int64, to_ts : Int64) : Summary
      count = 0_u64
      latencies = [] of Int64
      latency_sum = 0_i64
      redeliveries_sum = 0_u64
      redeliveries_max = 0_u32
      histogram = [0_u64, 0_u64, 0_u64, 0_u64]
      payload_sum = 0_u64
      each_segment_newest_first do |path|
        next unless segment_overlaps?(path, from_ts, to_ts)
        scan_segment(path) do |rec|
          next if rec.ack_ts_ms < from_ts
          next if rec.ack_ts_ms > to_ts
          count += 1
          if rec.latency_ms >= 0
            latencies << rec.latency_ms
            latency_sum += rec.latency_ms
          end
          redeliveries_sum += rec.redelivery_count
          redeliveries_max = rec.redelivery_count if rec.redelivery_count > redeliveries_max
          histogram[bucket_for(rec.redelivery_count)] += 1
          payload_sum += rec.payload_size
        end
      end
      latencies.sort!
      Summary.new(
        count: count,
        latency_p50: percentile(latencies, 0.50),
        latency_p95: percentile(latencies, 0.95),
        latency_p99: percentile(latencies, 0.99),
        latency_avg: latencies.empty? ? 0_i64 : (latency_sum // latencies.size),
        redeliveries_avg: count.zero? ? 0.0 : redeliveries_sum.to_f / count,
        redeliveries_max: redeliveries_max,
        redeliveries_histogram: histogram,
        payload_size_avg: count.zero? ? 0_u64 : payload_sum // count,
        dropped: @dropped.get(:relaxed),
      )
    end

    def close : Nil
      return if @closed.swap(true)
      @stopped.set(true)
      @flusher_done.receive?
      @write_file.try &.close
      @write_file = nil
    end

    def delete_files : Nil
      close
      Dir.glob(File.join(@data_dir, "#{FILE_PREFIX}*")).each do |path|
        File.delete?(path)
      end
    end

    private def run_flusher : Nil
      buffer = Array(Record).new(BATCH_MAX)
      loop do
        select
        when rec = @ch.receive
          buffer << rec
          while buffer.size < BATCH_MAX
            select
            when more = @ch.receive
              buffer << more
            else
              break
            end
          end
          write_batch(buffer)
          buffer.clear
        when timeout FLUSH_INTERVAL
          maybe_run_retention_sweep
        when @stopped.when_true.receive?
          drain_remaining(buffer)
          @flusher_done.send(nil)
          return
        end
      end
    rescue ex
      Log.error(exception: ex) { "ProcessedLog flusher crashed for #{@data_dir}" }
      @flusher_done.send(nil) rescue nil
    end

    private def drain_remaining(buffer : Array(Record)) : Nil
      loop do
        select
        when rec = @ch.receive
          buffer << rec
          if buffer.size >= BATCH_MAX
            write_batch(buffer)
            buffer.clear
          end
        else
          break
        end
      end
      write_batch(buffer) unless buffer.empty?
      buffer.clear
    end

    private def write_batch(buffer : Array(Record)) : Nil
      return if buffer.empty?
      file = @write_file || return
      last_ts = buffer.last.ack_ts_ms
      if @write_first_ts == 0
        @write_first_ts = buffer.first.ack_ts_ms
        seek_and_write_header_ts(file, offset: HEADER_SIZE - 16, value: @write_first_ts)
      end
      file.seek(0, IO::Seek::End)
      buffer.each do |rec|
        encode_record(file, rec)
      end
      file.flush
      @write_size = file.size.to_i64
      seek_and_write_header_ts(file, offset: HEADER_SIZE - 8, value: last_ts)
      rotate_if_full
    rescue ex : IO::Error
      Log.error(exception: ex) { "Failed to write processed_log batch for #{@data_dir}" }
    end

    private def seek_and_write_header_ts(file : File, offset : Int32, value : Int64) : Nil
      file.seek(offset, IO::Seek::Set)
      file.write_bytes(value, IO::ByteFormat::SystemEndian)
      file.flush
    end

    private def rotate_if_full : Nil
      return if @write_size < @segment_size
      file = @write_file
      if file
        file.close
        @write_file = nil
      end
      @write_segment_id += 1
      open_new_write_segment
    end

    private def open_or_create_write_segment : Nil
      Dir.mkdir_p @data_dir unless Dir.exists?(@data_dir)
      ids = list_segment_ids
      if ids.empty?
        @write_segment_id = 0
        open_new_write_segment
      else
        @write_segment_id = ids.max
        path = segment_path(@write_segment_id)
        file = File.new(path, "r+")
        @write_size = file.size.to_i64
        if @write_size < HEADER_SIZE
          # corrupt/truncated header — start fresh segment after it
          file.close
          @write_segment_id += 1
          open_new_write_segment
        else
          @write_first_ts = read_first_ts(file)
          @write_file = file
        end
      end
    end

    private def open_new_write_segment : Nil
      path = segment_path(@write_segment_id)
      file = File.new(path, "w+")
      file.write(MAGIC)
      file.write_bytes(VERSION, IO::ByteFormat::SystemEndian)
      file.write_bytes(0_i64, IO::ByteFormat::SystemEndian) # first_ts placeholder
      file.write_bytes(0_i64, IO::ByteFormat::SystemEndian) # last_ts placeholder
      file.flush
      @write_size = file.size.to_i64
      @write_first_ts = 0_i64
      @write_file = file
    end

    private def segment_path(id : UInt64) : String
      File.join(@data_dir, "#{FILE_PREFIX}#{id.to_s.rjust(10, '0')}")
    end

    private def list_segment_ids : Array(UInt64)
      Dir.glob(File.join(@data_dir, "#{FILE_PREFIX}*")).compact_map do |path|
        File.basename(path).lchop(FILE_PREFIX).to_u64?
      end
    end

    private def encode_record(io : IO, rec : Record) : Nil
      ex_bytes = rec.exchange.byte_slice(0, Math.min(rec.exchange.bytesize, 255)).to_slice
      rk_bytes = rec.routing_key.byte_slice(0, Math.min(rec.routing_key.bytesize, 255)).to_slice
      ct_bytes = rec.consumer_tag.byte_slice(0, Math.min(rec.consumer_tag.bytesize, 255)).to_slice
      io.write_bytes(rec.ack_ts_ms, IO::ByteFormat::SystemEndian)
      io.write_bytes(rec.latency_ms, IO::ByteFormat::SystemEndian)
      io.write_bytes(rec.payload_size, IO::ByteFormat::SystemEndian)
      io.write_bytes(rec.redelivery_count, IO::ByteFormat::SystemEndian)
      io.write_byte(ex_bytes.size.to_u8)
      io.write_byte(rk_bytes.size.to_u8)
      io.write_byte(ct_bytes.size.to_u8)
      io.write_byte(0_u8)
      io.write(ex_bytes)
      io.write(rk_bytes)
      io.write(ct_bytes)
    end

    private def scan_segment(path : String, & : Record -> Nil) : Nil
      File.open(path, "r") do |f|
        magic = Bytes.new(4)
        return unless f.read_fully?(magic) && magic == MAGIC
        version = UInt32.from_io(f, IO::ByteFormat::SystemEndian)
        return unless version == VERSION
        f.skip(16) # first_ts + last_ts
        loop do
          break if f.pos >= f.size
          ack_ts = Int64.from_io(f, IO::ByteFormat::SystemEndian)
          latency = Int64.from_io(f, IO::ByteFormat::SystemEndian)
          payload_size = UInt32.from_io(f, IO::ByteFormat::SystemEndian)
          redel = UInt32.from_io(f, IO::ByteFormat::SystemEndian)
          ex_len = f.read_byte || break
          rk_len = f.read_byte || break
          ct_len = f.read_byte || break
          f.skip(1) # reserved
          ex_buf = Bytes.new(ex_len)
          rk_buf = Bytes.new(rk_len)
          ct_buf = Bytes.new(ct_len)
          f.read_fully(ex_buf) if ex_len > 0
          f.read_fully(rk_buf) if rk_len > 0
          f.read_fully(ct_buf) if ct_len > 0
          yield Record.new(
            ack_ts_ms: ack_ts,
            latency_ms: latency,
            payload_size: payload_size,
            redelivery_count: redel,
            exchange: String.new(ex_buf),
            routing_key: String.new(rk_buf),
            consumer_tag: String.new(ct_buf),
          )
        end
      end
    rescue ex : IO::Error
      Log.warn(exception: ex) { "Failed scanning processed_log segment #{path}" }
    end

    private def segment_overlaps?(path : String, from_ts : Int64, to_ts : Int64) : Bool
      first_ts, last_ts = read_header_range(path)
      return false if last_ts > 0 && last_ts < from_ts
      return false if first_ts > 0 && first_ts > to_ts
      true
    end

    private def read_header_range(path : String) : Tuple(Int64, Int64)
      File.open(path, "r") do |f|
        magic = Bytes.new(4)
        return {0_i64, 0_i64} unless f.read_fully?(magic) && magic == MAGIC
        version = UInt32.from_io(f, IO::ByteFormat::SystemEndian)
        return {0_i64, 0_i64} unless version == VERSION
        first_ts = Int64.from_io(f, IO::ByteFormat::SystemEndian)
        last_ts = Int64.from_io(f, IO::ByteFormat::SystemEndian)
        return {first_ts, last_ts}
      end
    rescue
      {0_i64, 0_i64}
    end

    private def read_first_ts(file : File) : Int64
      file.seek(HEADER_SIZE - 16, IO::Seek::Set)
      ts = Int64.from_io(file, IO::ByteFormat::SystemEndian)
      file.seek(0, IO::Seek::End)
      ts
    end

    private def each_segment_newest_first(& : String -> Nil) : Nil
      list_segment_ids.sort.reverse_each do |id|
        yield segment_path(id)
      end
    end

    private def maybe_run_retention_sweep : Nil
      now = Time.instant
      return if (now - @last_retention_sweep) < RETENTION_SWEEP_INTERVAL
      @last_retention_sweep = now
      run_retention_sweep
    end

    private def run_retention_sweep : Nil
      cutoff = Time.utc.to_unix_ms - @retention_ms
      current = @write_segment_id
      list_segment_ids.each do |id|
        next if id == current
        path = segment_path(id)
        _, last_ts = read_header_range(path)
        next if last_ts == 0 # never written or unreadable; leave it
        File.delete(path) if last_ts < cutoff
      end
    rescue ex
      Log.warn(exception: ex) { "Retention sweep failed for #{@data_dir}" }
    end

    private def bucket_for(count : UInt32) : Int32
      case count
      when 0    then 0
      when 1..3 then 1
      when 4..7 then 2
      else           3
      end
    end

    private def percentile(sorted : Array(Int64), p : Float64) : Int64
      return 0_i64 if sorted.empty?
      idx = (sorted.size * p).to_i
      idx = sorted.size - 1 if idx >= sorted.size
      sorted[idx]
    end
  end
end
