require "../replication"
require "./file_index"
require "./replicator"
require "./follower"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"
require "lz4"

module LavinMQ
  module Replication
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
      include FileIndex
      include Replicator
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

      def append(path : String, file : MFile, position : Int32, length : Int32)
        return if @followers.empty?
        append path, FileRange.new(file, position, length)
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
      rescue ex : InvalidStartHeaderError
        Log.warn { ex.message }
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
          done = Channel({Follower, Bool}).new
          @followers.each do |f|
            spawn { f.close(done) }
          end
          @followers.size.times do
            follower, synced = done.receive
            Log.info { "Follower #{follower.name} stopped, in sync: #{synced}" }
          end
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
    end
  end
end
