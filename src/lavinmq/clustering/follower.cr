require "./actions"
require "./file_index"
require "../config"
require "socket"
require "wait_group"

module LavinMQ
  module Clustering
    class Follower
      Log = LavinMQ::Log.for "clustering.follower"

      @acked_bytes = 0_i64
      @sent_bytes = 0_i64
      @actions = Channel(Action).new(Config.instance.clustering_max_unsynced_actions)
      @running = WaitGroup.new
      getter id = -1
      getter remote_address

      def initialize(@socket : TCPSocket, @data_dir : String, @file_index : FileIndex)
        @socket.sync = true # Use buffering in lz4
        @socket.read_buffering = true
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

      def action_loop(lz4 = @lz4)
        @socket.tcp_nodelay = true
        @socket.read_buffering = false
        @running.add
        while action = @actions.receive?
          action.send(lz4, Log)
          sent_bytes = action.lag_size.to_i64
          while action2 = @actions.try_receive?
            action2.send(lz4, Log)
            sent_bytes += action2.lag_size
          end
          lz4.flush
          sync(sent_bytes)
        end
      ensure
        @actions.close
        @running.done
      end

      private def sync(bytes, socket = @socket) : Nil
        until bytes.zero?
          bytes -= read_ack(socket)
        end
      end

      private def read_ack(socket = @socket) : Int64
        len = socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        @acked_bytes += len
        len
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
        Log.info { "Calculating hashes" }
        count = 0
        @file_index.files_with_hash do |path, hash|
          count &+= 1
          lz4.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
          lz4.write path.to_slice
          lz4.write hash
        end
        lz4.write_bytes 0i32 # 0 means end of file list
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
          Log.info { "#{filename} requested" }
        end
        Log.info { "#{requested_files.size} files requested" }
        total_requested_bytes = requested_files.sum { |p| File.size File.join(@data_dir, p) }
        sent_bytes = 0i64
        start = Time.monotonic
        requested_files.each do |filename|
          file_size = send_requested_file(filename)

          sent_bytes += file_size
          total_requested_bytes -= file_size
          total_time_taken = (Time.monotonic - start).total_seconds
          bps = (sent_bytes / total_time_taken).round.to_u64
          time_left = (total_requested_bytes / bps).round(1)
          Log.info { "Uploaded #{filename} in #{bps.humanize_bytes}/s" }
          Log.info { "#{total_requested_bytes.humanize_bytes} left, expected #{time_left}s left" }
          Fiber.yield
        end
        @lz4.flush
      end

      private def send_requested_file(filename) : Int
        @file_index.with_file(filename) do |f|
          case f
          when MFile
            size = f.size.to_i64
            @lz4.write_bytes size, IO::ByteFormat::LittleEndian
            f.copy_to(@lz4, size)
            size
          when File
            size = f.size.to_i64
            @lz4.write_bytes size, IO::ByteFormat::LittleEndian
            IO.copy(f, @lz4, size) == size || raise IO::EOFError.new
            size
          when nil
            @lz4.write_bytes 0i64
            0
          else raise "unexpected file type #{f.class}"
          end
        end
      end

      def replace(path) : Int64
        send_action ReplaceAction.new(@data_dir, path)
      end

      def append(path, obj) : Int64
        send_action AppendAction.new(@data_dir, path, obj)
      end

      def delete(path) : Int64
        send_action DeleteAction.new(@data_dir, path)
      end

      private def send_action(action : Action) : Int64
        lag_size = action.lag_size
        @sent_bytes += lag_size
        @actions.send action
        lag_size
      end

      def close
        @actions.close
        @running.wait # let action_loop finish

        # abort remaining actions (unmap pending files)
        while action = @actions.receive?
          action.done
        end

        begin
          @lz4.close
          @socket.close
        rescue IO::Error
          # ignore connection errors while closing
        end
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
    end
  end
end
