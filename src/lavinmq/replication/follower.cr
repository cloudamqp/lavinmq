require "../replication"
require "./actions"
require "./file_index"

module LavinMQ
  module Replication
    class Follower
      Log = ::Log.for(self)

      @acked_bytes = 0_i64
      @sent_bytes = 0_i64
      @actions = Channel(Action).new(4096)
      getter name
      getter? closed

      def initialize(@socket : TCPSocket, @file_index : FileIndex)
        Log.context.set(address: @socket.remote_address.to_s)
        @name = @socket.remote_address.to_s
        @closed = false
        @socket.write_timeout = 5.seconds
        @socket.read_timeout = 5.seconds
        @socket.buffer_size = 64 * 1024
        @lz4 = Compress::LZ4::Writer.new(@socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
      end

      def negotiate!(password) : Nil
        validate_header!
        authenticate!(password)
        @socket.read_timeout = nil
        if keepalive = Config.instance.tcp_keepalive
          @socket.keepalive = true
          @socket.tcp_keepalive_idle = keepalive[0]
          @socket.tcp_keepalive_interval = keepalive[1]
          @socket.tcp_keepalive_count = keepalive[2]
        end
        Log.info { "Accepted" }
      end

      def full_sync : Nil
        Log.info { "Calculating hashes" }
        send_file_list
        Log.info { "File list sent" }
        send_requested_files
      end

      def read_acks(socket = @socket) : Nil
        spawn action_loop, name: "Follower#action_loop"
        loop do
          read_ack(socket)
        end
      rescue IO::Error
      end

      def read_ack(socket = @socket) : Nil
        len = socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
        @acked_bytes += len
      end

      private def action_loop(socket = @lz4)
        while action = @actions.receive?
          @sent_bytes += action.send(socket)
          while action2 = @actions.try_receive?
            @sent_bytes += action2.send(socket)
          end
          socket.flush
        end
      rescue IO::Error
      ensure
        close
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
          path = path[Config.instance.data_dir.bytesize + 1..]
          socket.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
          socket.write path.to_slice
          socket.write hash
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
        send_action AddAction.new(path, mfile)
      end

      def append(path, obj) : Int64
        send_action AppendAction.new(path, obj)
      end

      def delete(path) : Int64
        send_action DeleteAction.new(path)
      end

      private def send_action(action : Action) : Int64
        action_size = action.bytesize
        @sent_bytes += action_size
        @actions.send action
        action_size
      end

      def close(synced_close : Channel({Follower, Bool})? = nil)
        return if closed?
        @closed = true
        Log.info { "Disconnected" }
        wait_for_sync if synced_close
        @actions.close
        @lz4.close
        @socket.close
      rescue IO::Error
        # ignore connection errors while closing
      ensure
        synced_close.try &.send({self, lag.zero?})
      end

      def to_json(json : JSON::Builder)
        {
          remote_address:     @socket.remote_address.to_s,
          sent_bytes:         @sent_bytes,
          acked_bytes:        @acked_bytes,
          compression_ratio:  @lz4.compression_ratio,
          uncompressed_bytes: @lz4.uncompressed_bytes,
          compressed_bytes:   @lz4.compressed_bytes,
        }.to_json(json)
      end

      def lag : Int64
        @sent_bytes - @acked_bytes
      end

      private def wait_for_sync
        in_sync = Channel(Nil).new
        spawn do
          until lag.zero? || @socket.closed?
            Fiber.yield
          end
          in_sync.send nil
        end
        in_sync.receive
      end
    end
  end
end
