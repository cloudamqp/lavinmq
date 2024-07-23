require "../clustering"
require "./file_index"
require "./replicator"
require "./follower"
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
      Log = ::Log.for("clustering.server")

      @lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new(4)
      @has_followers = Channel(Int32).new
      @password : String
      @files = Hash(String, MFile?).new
      @dirty_isr = true
      @id : Int32
      @config : Config

      def initialize(config : Config, @etcd : Etcd, @id = File.read(File.join(config.data_dir, ".clustering_id")).to_i(36))
        Log.info { "ID: #{@id.to_s(36)}" }
        @config = config
        @data_dir = @config.data_dir
        @password = password
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
        append path, FileRange.new(file, position, length)
      end

      def append(path : String, obj)
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
        path = File.join(@data_dir, filename)
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

      def password : String
        key = "#{@config.clustering_etcd_prefix}/clustering_secret"
        @etcd.get(key) ||
          begin
            Log.info { "Generating new clustering secret" }
            secret = Random::Secure.base64(32)
            @etcd.put(key, secret)
            secret
          end
      end

      @listeners = Array(TCPServer).new(1)

      def listen(server : TCPServer)
        server.listen
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
        while @has_followers.try_send? @followers.size
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
        Log.warn { "Follower disonnected: #{ex.message}" }
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
        @has_followers.close
        Fiber.yield # required for follower/listener fibers to actually finish
      end

      private def each_follower(& : Follower -> Nil) : Nil
        isr_count = @followers.size
        return if @listeners.empty? # Don't require ISRs before the server has started and followers can connect
        
        until isr_count >= @config.clustering_min_isr
          Log.warn { "ISR requirement not met (#{isr_count}/#{@config.clustering_min_isr})" }
          isr_count = @has_followers.receive? || return
        end
        @lock.synchronize do
          update_isr if @dirty_isr
          @followers.each do |f|
            yield f
          rescue Channel::ClosedError
            Fiber.yield # Allow other fiber to run to remove the follower from array
          end
        end
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

      def append(path : String, file : MFile, position : Int32, length : Int32)
      end

      def append(path : String, obj)
      end

      def delete_file(path : String)
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
