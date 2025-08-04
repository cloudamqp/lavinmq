require "../clustering"
require "./file_index"
require "./replicator"
require "./follower"
require "./checksums"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"
require "lz4"
require "../etcd"

module LavinMQ
  module Clustering
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
      Log = LavinMQ::Log.for "clustering.server"

      @lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new(4)
      @password : String
      @files = Hash(String, MFile?).new
      @dirty_isr = true
      @id : Int32
      @config : Config

      def initialize(config : Config, @etcd : Etcd, @id : Int32)
        Log.info { "ID: #{@id.to_s(36)}" }
        @config = config
        @data_dir = @config.data_dir
        @password = password
        @checksums = Checksums.new(@data_dir)
      end

      def clear
        @files.clear
        @checksums.clear
      end

      def register_file(file : File)
        path = strip_datadir file.path
        @files[path] = nil
      end

      def register_file(mfile : MFile)
        path = strip_datadir mfile.path
        @files[path] = mfile
      end

      def replace_file(path : String) # only non mfiles are ever replaced
        path = strip_datadir path
        @files[path] = nil
        @checksums.delete(path)
        each_follower &.replace(path)
      end

      def append(path : String, obj)
        path = strip_datadir path
        @checksums.delete(path)
        each_follower &.append(path, obj)
      end

      def delete_file(path : String, wg)
        path = strip_datadir path
        @files.delete(path)
        @checksums.delete(path)
        each_follower &.delete(path, wg)
      end

      def nr_of_files
        @files.size
      end

      def files_with_hash(& : Tuple(String, Bytes) -> Nil)
        sha1 = Digest::SHA1.new
        @files.each do |path, mfile|
          if calculated_hash = @checksums[path]?
            yield({path, calculated_hash})
          else
            if file = mfile
              sha1.update file.to_slice
              file.dontneed
            else
              filename = File.join(@data_dir, path)
              next unless File.exists? filename
              sha1.file filename
            end
            hash = sha1.final
            @checksums[path] = hash
            sha1.reset
            Fiber.yield # CPU bound so allow other fibers to run here
            yield({path, hash})
          end
        end
      end

      def with_file(filename, & : MFile | File | Nil -> _)
        if @files.has_key? filename
          if mfile = @files[filename]
            yield mfile
          else
            path = File.join(@data_dir, filename)
            if File.exists? path
              File.open(path) do |f|
                f.read_buffering = false
                yield f
              end
            else
              yield nil
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

      def password : String
        key = "#{@config.clustering_etcd_prefix}/clustering_secret"
        secret = Random::Secure.base64(32)
        stored_secret = @etcd.put_or_get(key, secret)
        if stored_secret == secret
          Log.info { "Generated new clustering secret" }
        end
        stored_secret
      end

      @listeners = Array(TCPServer).new(1)

      def listen(server : TCPServer)
        server.listen
        @checksums.restore
        Log.info { "Listening on #{server.local_address}" }
        @listeners << server
        while socket = server.accept?
          spawn handle_socket(socket), name: "Clustering follower"
        end
      end

      private def handle_socket(socket : TCPSocket)
        Log.context.set(follower: socket.remote_address.to_s)
        follower = Follower.new(socket, @data_dir, self)
        follower.negotiate!(@password)
        if follower.id == @id
          Log.error { "Disconnecting follower with the clustering id of the leader" }
          return
        end
        if stale_follower = @followers.find { |f| f.id == follower.id }
          Log.error { "Disconnecting stale follower with id #{follower.id.to_s(36)}" }
          stale_follower.close
        end
        follower.full_sync # sync the bulk
        @lock.synchronize do
          follower.full_sync # sync the last
          @followers << follower
          update_isr
        end
        begin
          follower.action_loop
        ensure
          @lock.synchronize do
            @followers.delete(follower)
            @dirty_isr = true
          end
        end
      rescue ex : AuthenticationError
        Log.warn { "Follower negotiation error" }
      rescue ex : InvalidStartHeaderError
        Log.warn { ex.message }
      rescue ex : IO::EOFError
        Log.info { "Follower disconnected" }
      rescue ex : IO::Error
        Log.warn(exception: ex) { "Follower disonnected: #{ex.message}" }
      ensure
        follower.try &.close
      end

      private def update_isr
        isr_key = "#{@config.clustering_etcd_prefix}/isr"
        ids = String.build do |str|
          @followers.each { |f| f.id.to_s(str, 36); str << "," }
          @id.to_s(str, 36)
        end
        Log.info { "In-sync replicas: #{ids}" }
        @etcd.put(isr_key, ids)
        @dirty_isr = false
      end

      def close
        @listeners.each &.close
        @lock.synchronize do
          @followers.each &.close
          @followers.clear
        end
        Fiber.yield # required for follower/listener fibers to actually finish
        @checksums.store
      end

      private def each_follower(& : Follower -> Nil) : Nil
        @lock.synchronize do
          update_isr if @dirty_isr
          @followers.each do |f|
            yield f
          rescue Channel::ClosedError
            Fiber.yield # Allow other fiber to run to remove the follower from array
          end
        end
      end

      private def strip_datadir(path : String) : String
        path[@data_dir.bytesize + 1..]
      end
    end

    class NoopServer
      include Replicator

      def register_file(file : File)
      end

      def register_file(mfile : MFile)
      end

      def replace_file(path : String) # only non mfiles are ever replaced
      end

      def append(path : String, obj)
      end

      def delete_file(path : String, wg : WaitGroup)
      end

      def followers : Array(Follower)
        Array(Follower).new(0)
      end

      def close
      end

      def listen(server : TCPServer)
      end

      def clear
      end

      def password : String
        ""
      end
    end
  end
end
