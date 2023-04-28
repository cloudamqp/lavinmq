require "../replication"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"

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
      @files = Set(String).new
      @mfiles = Hash(String, MFile).new

      def initialize
        @password = password
        @tcp = TCPServer.new
        @tcp.sync = false
        @tcp.reuse_address = true
      end

      def clear
        @files.clear
        @mfiles.clear
      end

      def add_file(path : String)
        @files << path
        each_follower &.add(path, nil)
      end

      def add_file(mfile : MFile)
        @mfiles[mfile.path] = mfile
        each_follower &.add(mfile.path, mfile)
      end

      def append(path, obj)
        each_follower &.append(path, obj)
      end

      def delete_file(path)
        @files.delete(path)
        each_follower &.delete(path)
      end

      def delete_file(mfile : MFile)
        @mfiles.delete(mfile.path)
        each_follower &.delete(mfile.path)
      end

      def files_with_hash(& : Tuple(String, Bytes) -> Nil)
        sha1 = Digest::SHA1.new
        hash = Bytes.new(sha1.digest_size)
        @files.each do |path|
          sha1.file path
          sha1.final(hash)
          sha1.reset
          yield({path, hash})
        end

        @mfiles.each_value do |mfile|
          sha1.update mfile.to_slice
          sha1.final(hash)
          sha1.reset
          yield({mfile.path, hash})
        end
      end

      def with_file(filename, & : MFile | File | Nil -> Nil) : Nil
        path = File.join(Config.instance.data_dir, filename)
        if @files.includes? path
          File.open(path) do |f|
            yield f
          end
        elsif mfile = @mfiles[path]?
          yield mfile
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
      rescue ex : RuntimeError
        Log.warn(exception: ex) { "Follower negotiation error" }
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

        def initialize(@socket : TCPSocket, @server : Server)
          Log.context.set(address: @socket.remote_address.to_s)
          @socket.write_timeout = 5
          @socket.read_timeout = 5
          spawn action_loop, name: "Follower#action_loop"
        end

        def negotiate!(password) : Nil
          validate_header!
          authenticate!(password)
          Log.info { "Accepted" }
        end

        def full_sync : Nil
          send_file_list
          send_requested_files
          @socket.read_timeout = nil
        end

        def read_acks(socket = @socket) : Nil
          loop do
            len = socket.read_bytes(Int64, IO::ByteFormat::LittleEndian)
            @acked_bytes += len
            @ack.try_send nil
          end
        rescue IO::Error
        end

        private def action_loop(socket = @socket)
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
          else
            raise "Authentication failed"
          end
        end

        private def send_file_list
          @server.files_with_hash do |path, hash|
            path = path[Config.instance.data_dir.bytesize + 1..]
            @socket.write_bytes path.bytesize.to_i32, IO::ByteFormat::LittleEndian
            @socket.write path.to_slice
            @socket.write hash
          end
          @socket.write_bytes 0i32
          @socket.flush
        end

        private def send_requested_files
          loop do
            filename_len = @socket.read_bytes Int32, IO::ByteFormat::LittleEndian
            break if filename_len.zero?

            filename = @socket.read_string(filename_len)
            send_requested_file(filename)
          end
        end

        private def send_requested_file(filename)
          Log.debug { "#{filename} requested" }
          @server.with_file(filename) do |f|
            case f
            when MFile
              size = f.size.to_i64
              @socket.write_bytes size, IO::ByteFormat::LittleEndian
              @socket.write f.to_slice(0, size)
            when File
              size = f.size.to_i64
              @socket.write_bytes size, IO::ByteFormat::LittleEndian
              IO.copy f, @socket, size
            when nil
              @socket.write_bytes 0i64
            else raise "unexpected file type #{f.class}"
            end
            @socket.flush
          end
        end

        def add(path, obj)
          @actions.send AddAction.new(path, obj)
        end

        def append(path, obj)
          @actions.send AppendAction.new(path, obj)
        end

        def delete(path)
          @actions.send DeleteAction.new(path)
        end

        record FileRange, mfile : MFile, pos : Int32, len : Int32

        abstract struct Action
          def initialize(@path : String)
          end

          abstract def send(socket : Socket) : Int64

          private def send_filename(socket : Socket)
            filename = @path[Config.instance.data_dir.bytesize + 1..]
            socket.write_bytes filename.bytesize.to_i32, IO::ByteFormat::LittleEndian
            socket.write filename.to_slice
          end
        end

        struct AddAction < Action
          def initialize(@path : String, @obj : MFile?)
          end

          def send(socket) : Int64
            Log.debug { "Add #{@path}" }
            case obj = @obj
            in MFile
              send_filename(socket)
              len = obj.size.to_i64
              socket.write_bytes len, IO::ByteFormat::LittleEndian
              socket.write obj.to_slice(0, len)
              len
            in Nil
              File.open(@path) do |f|
                size = f.size.to_i64
                send_filename(socket)
                socket.write_bytes size, IO::ByteFormat::LittleEndian
                IO.copy(f, socket, size)
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
              socket.write obj.mfile.to_slice(obj.pos, obj.len)
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
          @socket.close
        rescue IO::Error
          # ignore connection errors while closing
        end

        def to_json(json : JSON::Builder)
          {
            remote_address: @socket.remote_address.to_s,
            sent_bytes:     @sent_bytes,
            acked_bytes:    @acked_bytes,
          }.to_json(json)
        end

        def lag : Int64
          @sent_bytes - @acked_bytes
        end
      end
    end
  end
end
