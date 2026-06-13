require "./file_index"
require "../config"
require "../rate_limiter"
require "socket"
require "wait_group"

module LavinMQ
  module Clustering
    class Follower
      enum State
        Syncing
        Synced
      end
      Log = LavinMQ::Log.for "clustering.follower"

      @acked_bytes = 0_i64
      @sent_bytes = 0_i64
      @write_lock = Mutex.new(:unchecked)
      @running = WaitGroup.new
      @state = State::Syncing
      getter id = -1
      getter remote_address
      getter state

      def initialize(@socket : TCPSocket, @data_dir : String, @file_index : FileIndex)
        @socket.sync = true # Use buffering in lz4
        @socket.read_buffering = true
        @socket.write_timeout = 3.seconds # don't wait for blocked followers
        @remote_address = @socket.remote_address
        @lz4 = Compress::LZ4::Writer.new(@socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
      end

      def negotiate!(password) : Nil
        @socket.read_timeout = 5.seconds # prevent idling non-authed sockets
        validate_header!
        authenticate!(password)
        @id = @socket.read_bytes Int32, IO::ByteFormat::LittleEndian
        if keepalive = Config.instance.tcp_keepalive
          @socket.keepalive = true
          @socket.tcp_keepalive_idle = keepalive[0]
          @socket.tcp_keepalive_interval = keepalive[1]
          @socket.tcp_keepalive_count = keepalive[2]
        end
        @socket.read_timeout = nil # assumed authed followers are well behaving
        Log.info { "Accepted ID #{@id.to_s(36)}" }
      end

      def full_sync : Nil
        send_file_list
        send_requested_files
      end

      def ack_loop
        @running.add
        @socket.read_timeout = 100.milliseconds # Wait for an ack max this time, otherwise flush the buffer to trigger acks
        loop do
          begin
            len = @socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
            @acked_bytes += len
          rescue IO::TimeoutError
            @write_lock.synchronize do
              @lz4.flush
            end
          end
        end
      rescue IO::EOFError | Socket::Error | IO::Error
        # socket closed
      ensure
        @running.done
      end

      private def validate_header! : Nil
        buf = uninitialized UInt8[8]
        slice = buf.to_slice
        @socket.read_fully(slice)
        if slice != Start
          @socket.write(Start)
          raise InvalidStartHeaderError.new(slice)
        end
      end

      private def authenticate!(password) : Nil
        len = @socket.read_bytes UInt8, IO::ByteFormat::LittleEndian
        client_password = @socket.read_string(len)
        if Crypto::Subtle.constant_time_compare(password, client_password)
          @socket.write_byte 0u8
        else
          @socket.write_byte 1u8
          raise AuthenticationError.new
        end
      end

      private def send_file_list(lz4 = @lz4)
        Log.info { "Calculating hashes for #{@file_index.nr_of_files} files" }
        count = 0
        log_limiter = RateLimiter.new(2.seconds)
        @file_index.files_with_hash do |path, hash|
          lz4.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
          lz4.write path.to_slice
          lz4.write hash
          count &+= 1
          log_limiter.do { Log.info { "Calculated hash for #{count}/#{@file_index.nr_of_files} files" } }
        end
        lz4.write_bytes 0i32 # 0 means end of file list (endian-agnostic)
        lz4.flush
        Log.info { "File list sent (#{count} files)" }
        count
      end

      private def send_requested_files(socket = @socket)
        requested_files = Array(String).new
        loop do
          filename_len = socket.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?
          filename = socket.read_string(filename_len)
          requested_files << filename
          Log.debug { "#{filename} requested" }
        end
        Log.info { "#{requested_files.size} files requested" }
        total_requested_bytes = requested_files.sum(0i64) do |p|
          @file_index.with_file(p) { |_f, size| size }
        end
        sent_bytes = 0i64
        uploaded_count = 0
        log_limiter = RateLimiter.new(2.seconds)
        start = Time.instant
        requested_files.each do |filename|
          file_size = send_requested_file(filename)

          sent_bytes += file_size
          uploaded_count &+= 1
          total_requested_bytes -= file_size
          total_time_taken = (Time.instant - start).total_seconds
          bps = (sent_bytes / total_time_taken).round.to_u64
          time_left = bps > 0 ? (total_requested_bytes / bps).round(1) : 0
          Log.debug { "Uploaded #{filename} at #{bps.humanize_bytes}/s" }
          log_limiter.do { Log.info { "Uploaded #{uploaded_count}/#{requested_files.size} files at #{bps.humanize_bytes}/s, #{total_requested_bytes.humanize_bytes} left (~#{time_left}s)" } }
          Fiber.yield
        end
        Log.info { "Uploaded all #{requested_files.size} files" } unless requested_files.empty?
        @lz4.flush
      end

      private def send_requested_file(filename) : Int
        @file_index.with_file(filename) do |f, size|
          if f
            @lz4.write_bytes size, IO::ByteFormat::LittleEndian
            IO.copy(f, @lz4, size) == size || raise IO::EOFError.new
            size
          else
            @lz4.write_bytes 0i64 # missing file marker (endian-agnostic)
            0
          end
        end
      end

      def replace(path) : Int64
        @write_lock.synchronize do
          File.open(File.join(@data_dir, path)) do |file|
            file_size = file.size
            lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + file_size).to_i64
            @sent_bytes += lag_size
            send_filename(path)
            @lz4.write_bytes file_size.to_i64, IO::ByteFormat::LittleEndian
            IO.copy(file, @lz4, file_size) == file_size || raise IO::EOFError.new
            lag_size
          end
        end
      end

      def append(path : String, bytes : Bytes) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + bytes.bytesize).to_i64
          @sent_bytes += lag_size
          send_filename(path)
          @lz4.write_bytes -bytes.bytesize.to_i64, IO::ByteFormat::LittleEndian
          @lz4.write bytes
          lag_size
        end
      end

      def append(path : String, value : UInt32 | Int32) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64) + 4).to_i64
          @sent_bytes += lag_size
          send_filename(path)
          @lz4.write_bytes -4i64, IO::ByteFormat::LittleEndian
          @lz4.write_bytes value, IO::ByteFormat::LittleEndian
          lag_size
        end
      end

      def delete(path) : Int64
        @write_lock.synchronize do
          lag_size = (sizeof(Int32) + path.bytesize + sizeof(Int64)).to_i64
          @sent_bytes += lag_size
          send_filename(path)
          @lz4.write_bytes 0i64 # delete marker (endian-agnostic)
          lag_size
        end
      end

      private def send_filename(path)
        @lz4.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
        @lz4.write path.to_slice
      end

      def close
        begin
          @write_lock.synchronize do
            @lz4.close
            @socket.close
          end
        rescue IO::Error
          # ignore connection errors while closing
        end
        @running.wait
      end

      def to_json(json : JSON::Builder)
        {
          remote_address:     @remote_address.to_s,
          sent_bytes:         @sent_bytes,
          acked_bytes:        @acked_bytes,
          lag_in_bytes:       lag_in_bytes,
          compression_ratio:  @lz4.compression_ratio,
          uncompressed_bytes: @lz4.uncompressed_bytes,
          compressed_bytes:   @lz4.compressed_bytes,
          id:                 @id.to_s(36),
        }.to_json(json)
      end

      def lag_in_bytes : Int64
        @sent_bytes - @acked_bytes
      end

      def syncing?
        @state.syncing?
      end

      def synced?
        @state.synced?
      end

      def mark_synced!
        @state = State::Synced
      end
    end
  end
end
