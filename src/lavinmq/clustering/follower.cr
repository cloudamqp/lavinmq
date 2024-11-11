require "./actions"
require "./file_index"
require "../config"
require "socket"

module LavinMQ
  module Clustering
    class Follower
      Log = LavinMQ::Log.for "clustering.follower"

      @acked_bytes = 0_i64
      @sent_bytes = 0_i64
      @actions = Channel(Action).new(Config.instance.clustering_max_unsynced_actions)
      @closed = false
      getter id = -1
      getter remote_address

      def initialize(@socket : TCPSocket, @data_dir : String, @file_index : FileIndex)
        @socket.write_timeout = 5.seconds
        @socket.read_timeout = 5.seconds
        @remote_address = @socket.remote_address
        @lz4 = Compress::LZ4::Writer.new(@socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
      end

      def negotiate!(password) : Nil
        validate_header!
        authenticate!(password)
        @id = @socket.read_bytes Int32, IO::ByteFormat::LittleEndian
        @socket.read_timeout = nil
        @socket.tcp_nodelay = true
        @socket.read_buffering = false
        @socket.sync = true # Use buffering in lz4
        if keepalive = Config.instance.tcp_keepalive
          @socket.keepalive = true
          @socket.tcp_keepalive_idle = keepalive[0]
          @socket.tcp_keepalive_interval = keepalive[1]
          @socket.tcp_keepalive_count = keepalive[2]
        end
        Log.info { "Accepted ID #{@id}" }
      end

      def full_sync : Nil
        Log.info { "Calculating hashes" }
        send_file_list
        Log.info { "File list sent" }
        send_requested_files
      end

      def action_loop(lz4 = @lz4)
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
        begin
          @lz4.close
          @socket.close
        rescue IO::Error
          # ignore connection errors while closing
        end
      end

      private def sync(bytes, socket = @socket) : Nil
        until bytes.zero?
          bytes -= read_ack(socket)
        end
      end

      private def read_ack(socket = @socket) : Int64
        len = socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        @acked_bytes += len
        if @closed && lag_in_bytes.zero?
          @closed_and_in_sync.close
        end
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
      ensure
        @socket.flush
      end

      private def send_file_list(socket = @lz4)
        @file_index.files_with_hash do |path, hash|
          path = path[@data_dir.bytesize + 1..]
          socket.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
          socket.write path.to_slice
          socket.write hash
          Fiber.yield
        end
        socket.write_bytes 0i32
        socket.flush
      end

      private def send_requested_files(socket = @socket)
        loop do
          filename_len = socket.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?

          filename = socket.read_string(filename_len)
          send_requested_file(filename)
          @lz4.flush
          Fiber.yield
        end
      end

      private def send_requested_file(filename)
        Log.info { "#{filename} requested" }
        @file_index.with_file(filename) do |f|
          case f
          when MFile
            size = f.size.to_i64
            @lz4.write_bytes size, IO::ByteFormat::LittleEndian
            f.copy_to(@lz4, size)
          when File
            size = f.size.to_i64
            @lz4.write_bytes size, IO::ByteFormat::LittleEndian
            IO.copy(f, @lz4, size) == size || raise IO::EOFError.new
          when nil
            @lz4.write_bytes 0i64
          else raise "unexpected file type #{f.class}"
          end
        end
      end

      def add(path, mfile : MFile? = nil) : Int64
        path = path[(@data_dir.size + 1)..] if Path[path].absolute?
        send_action AddAction.new(@data_dir, path, mfile)
      end

      def append(path, obj) : Int64
        path = path[(@data_dir.size + 1)..] if Path[path].absolute?
        send_action AppendAction.new(@data_dir, path, obj)
      end

      def delete(path) : Int64
        path = path[(@data_dir.size + 1)..] if Path[path].absolute?
        send_action DeleteAction.new(@data_dir, path)
      end

      private def send_action(action : Action) : Int64
        lag_size = action.lag_size
        @sent_bytes += lag_size
        @actions.send action
        lag_size
      end

      @closed_and_in_sync = Channel(Nil).new

      def close
        @closed = true
        @actions.close
        @closed_and_in_sync.receive? if lag_in_bytes > 0
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
