require "../replication"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"
{% unless flag?(:without_lz4) %}
  require "lz4"
{% end %}

module LavinMQ
  class Replication
    # When a follower connects:
    # It sends a static header (wrong header disconnects the client)
    # It sends its password (servers closes the connection if the password is wrong)
    # Server sends a list of files in its data directory and the sha1 hash of those files
    # Client requests files that is missing or has mismatching checksums of
    # In the meantime the server queues up changes (all publishes/consumes are paused)
    # When client doesn't request more files starts to stream changes
    # Server sends "appends", which include the file path and the bytes to be appended
    # It also sends which files should be deleted or has been rewritten (such as json files)
    # The follower sends back/acknowledges how many bytes it has received
    class Server
      Log = ::Log.for("replication")

      @lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new
      @password : String
      @files = Hash(String, MFile?).new

      def initialize
        @password = password
        @tcp = TCPServer.new
        @tcp.sync = false
        @tcp.reuse_address = true
      end

      def clear
        @files.clear
      end

      def register_file(file : File)
        @files[file.path] = nil
      end

      def register_file(mfile : MFile)
        @files[mfile.path] = mfile
      end

      def replace_file(path : String) # only non mfiles are ever replaced
        @files[path] = nil
        each_follower &.add(path)
      end

      def append(path : String, obj)
        Log.debug { "appending #{obj} to #{path}" }
        each_follower &.append(path, obj)
      end

      def delete_file(path : String)
        @files.delete(path)
        each_follower &.delete(path)
      end

      def files_with_hash(& : Tuple(String, Bytes) -> Nil)
        sha1 = Digest::SHA1.new
        hash = Bytes.new(sha1.digest_size)
        @files.each do |path, mfile|
          if mfile
            was_unmapped = mfile.unmapped?
            sha1.update mfile.to_slice
            mfile.unmap if was_unmapped
          else
            sha1.file path
          end
          sha1.final hash
          sha1.reset
          yield({path, hash})
        end
      end

      def with_file(filename, & : MFile | File | Nil -> Nil) : Nil
        path = File.join(Config.instance.data_dir, filename)
        if @files.has_key? path
          if mfile = @files[path]
            yield mfile
          else
            File.open(path) do |f|
              f.read_buffering = false
              yield f
            end
          end
        else
          yield nil
        end
      end

      def followers : Array(Follower)
        @lock.synchronize do
          @followers.dup # for thread safety
        end
      end

      private def password : String
        path = File.join(Config.instance.data_dir, ".replication_secret")
        begin
          info = File.info(path)
          raise "File permissions of #{path} has to be 0400" if info.permissions.value != 0o400
          raise "Secret in #{path} is too short (min 32 bytes)" if info.size < 32
          password = File.read(path).chomp
        rescue File::NotFoundError
          password = Random::Secure.base64(32)
          File.open(path, "w") do |f|
            f.chmod(0o400)
            f.print password
          end
        end
        password
      end

      def bind(host, port)
        @tcp.bind(host, port)
        self
      end

      def listen
        @tcp.listen
        Log.info { "Listening on #{@tcp.local_address}" }
        while socket = @tcp.accept?
          spawn handle_socket(socket), name: "Replication follower"
        end
      end

      def handle_socket(socket)
        follower = Follower.new(socket, self)
        follower.negotiate!(@password)
        @lock.synchronize do
          follower.full_sync
          @followers << follower
        end
        begin
          follower.read_acks
        ensure
          @lock.synchronize do
            @followers.delete(follower)
          end
        end
      rescue ex : AuthenticationError
        Log.warn { "Follower negotiation error" }
      rescue ex : IO::EOFError
        Log.info { "Follower disconnected" }
      rescue ex : IO::Error
        Log.warn(exception: ex) { "Follower connection error" } unless socket.closed?
      ensure
        follower.try &.close
      end

      def close
        @tcp.close
        @lock.synchronize do
          @followers.each &.close
          @followers.clear
        end
        Fiber.yield # required for follower/listener fibers to actually finish
      end

      private def each_follower(& : Follower -> Nil) : Nil
        @lock.synchronize do
          @followers.each do |f|
            yield f
          rescue Channel::ClosedError
            Fiber.yield # Allow other fiber to run to remove the follower from array
          end
        end
      end

      class Follower
        Log = ::Log.for(self)

        getter ack = Channel(Nil).new
        @acked_bytes = 0_i64
        @sent_bytes = 0_i64
        @actions = Channel(Action).new(4096)
        @lz4 : IO

        def initialize(@socket : TCPSocket, @server : Server)
          Log.context.set(address: @socket.remote_address.to_s)
          @socket.write_timeout = 5
          @socket.read_timeout = 5
          @socket.buffer_size = 64 * 1024
          @lz4 = @socket
          {% unless flag?(:without_lz4) %}
            @lz4 = Compress::LZ4::Writer.new(@socket, Compress::LZ4::CompressOptions.new(auto_flush: false, block_mode_linked: true))
          {% end %}
        end

        def negotiate!(password) : Nil
          validate_header!
          authenticate!(password)
          @socket.read_timeout = nil
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
            len = socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
            @acked_bytes += len
            @ack.try_send nil
          end
        rescue IO::Error
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
            raise IO::Error.new("Invalid start header: #{slice}")
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
          @server.files_with_hash do |path, hash|
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
          end
          @lz4.flush
        end

        private def send_requested_file(filename)
          Log.info { "#{filename} requested" }
          @server.with_file(filename) do |f|
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

        def add(path, mfile : MFile? = nil)
          @actions.send AddAction.new(path, mfile)
        end

        def append(path, obj)
          @actions.send AppendAction.new(path, obj)
        end

        def delete(path)
          @actions.send DeleteAction.new(path)
        end

        record FileRange, mfile : MFile, pos : Int32, len : Int32 do
          def to_slice : Bytes
            mfile.to_slice(pos, len)
          end
        end

        abstract struct Action
          def initialize(@path : String)
          end

          abstract def send(socket : IO) : Int64

          private def send_filename(socket : IO)
            filename = @path[Config.instance.data_dir.bytesize + 1..]
            socket.write_bytes filename.bytesize.to_i32, IO::ByteFormat::LittleEndian
            socket.write filename.to_slice
          end
        end

        struct AddAction < Action
          def initialize(@path : String, @mfile : MFile? = nil)
          end

          def send(socket) : Int64
            Log.debug { "Add #{@path}" }
            send_filename(socket)
            if mfile = @mfile
              size = mfile.size.to_i64
              socket.write_bytes size, IO::ByteFormat::LittleEndian
              mfile.copy_to(socket, size)
              size
            else
              File.open(@path) do |f|
                size = f.size.to_i64
                socket.write_bytes size, IO::ByteFormat::LittleEndian
                IO.copy(f, socket, size) == size || raise IO::EOFError.new
                size
              end
            end
          end
        end

        struct AppendAction < Action
          def initialize(@path : String, @obj : Bytes | FileRange | UInt32 | Int32)
          end

          def send(socket) : Int64
            send_filename(socket)
            len : Int64
            case obj = @obj
            in Bytes
              len = obj.bytesize.to_i64
              socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
              socket.write obj
            in FileRange
              len = obj.len.to_i64
              socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
              socket.write obj.to_slice
            in UInt32, Int32
              len = 4i64
              socket.write_bytes -len.to_i64, IO::ByteFormat::LittleEndian
              socket.write_bytes obj, IO::ByteFormat::LittleEndian
            end
            Log.debug { "Append #{len} bytes to #{@path}" }
            len
          end
        end

        struct DeleteAction < Action
          def send(socket) : Int64
            Log.debug { "Delete #{@path}" }
            send_filename(socket)
            socket.write_bytes 0i64
            0i64
          end
        end

        def close
          Log.info { "Disconnected" }
          @actions.close
          @lz4.close
          @socket.close
        rescue IO::Error
          # ignore connection errors while closing
        end

        def to_json(json : JSON::Builder)
          {% unless flag?(:without_lz4) %}
            {
              remote_address:     @socket.remote_address.to_s,
              sent_bytes:         @sent_bytes,
              acked_bytes:        @acked_bytes,
              compression_ratio:  @lz4.as(Compress::LZ4::Writer).compression_ratio,
              uncompressed_bytes: @lz4.as(Compress::LZ4::Writer).uncompressed_bytes,
              compressed_bytes:   @lz4.as(Compress::LZ4::Writer).compressed_bytes,
            }.to_json(json)
          {% else %}
            {
              remote_address: @socket.remote_address.to_s,
              sent_bytes:     @sent_bytes,
              acked_bytes:    @acked_bytes,
            }.to_json(json)
          {% end %}
        end

        def lag : Int64
          @sent_bytes - @acked_bytes
        end
      end

      class Error < Exception; end

      class AuthenticationError < Error
        def initialize
          super("Authentication error")
        end
      end
    end
  end
end
