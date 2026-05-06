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
require "sync/shared"
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
      @sync_lock = Mutex.new(:unchecked)
      @followers = Array(Follower).new(4)
      @password : String
      @dirty_isr = true
      @id : Int32
      @config : Config
      @file_index : Sync::Shared(Tuple(Hash(String, MFile?), Checksums))

      def initialize(config : Config, @etcd : Etcd, @id : Int32)
        Log.info { "ID: #{@id.to_s(36)}" }
        @config = config
        @data_dir = @config.data_dir
        @password = password
        @file_index = Sync::Shared.new({Hash(String, MFile?).new, Checksums.new(@data_dir)}, :unchecked)
      end

      def clear
        @file_index.lock do |files, checksums|
          files.clear
          checksums.clear
        end
      end

      def register_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
      end

      def register_file(file : File)
        path = strip_datadir file.path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
      end

      def register_file(mfile : MFile)
        path = strip_datadir mfile.path
        @file_index.lock do |files, checksums|
          files[path] = mfile
          checksums.delete(path)
        end
      end

      def replace_file(path : String) # only non mfiles are ever replaced
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files[path] = nil
          checksums.delete(path)
        end
        each_follower &.replace(path)
      end

      def append(path : String, obj : Bytes | UInt32 | Int32)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower &.append(path, obj)
      end

      def delete_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files.delete(path)
          checksums.delete(path)
        end
        each_follower &.delete(path)
      end

      def nr_of_files
        @file_index.shared { |files, _checksums| files.size }
      end

      def files_with_hash(& : Tuple(String, Bytes) -> Nil)
        # Snapshot @files to allow safe iteration without holding the lock,
        # as other threads may mutate @files concurrently
        snapshot = @file_index.shared { |files, _checksums| files.dup }
        sha1 = Digest::SHA1.new
        snapshot.each do |path, _|
          cached_hash = @file_index.shared { |_files, checksums| checksums[path]? }
          if cached_hash
            yield({path, cached_hash})
          else
            # Hold shared lock during SHA1 computation to prevent the file
            # from being deleted or unmapped while we're reading it
            hash = @file_index.shared do |files, _checksums|
              # has_key? needed because `files[path]? == nil` means both "not found" and "non-MFile"
              next unless files.has_key?(path)
              if file = files[path]?
                sha1.update file.to_slice
                file.dontneed
              else
                filename = File.join(@data_dir, path)
                next unless File.exists? filename
                sha1.file filename
              end
              sha1.final.tap { sha1.reset }
            end
            next unless hash
            @file_index.lock { |_files, checksums| checksums[path] = hash }
            sleep 1.milliseconds # allow publishing threads to acquire exclusive lock between files
            yield({path, hash})
          end
        end
      end

      def with_file(filename, & : MFile | File | Nil -> _)
        # has_key? needed because `files[filename]? == nil` means both "not found" and "non-MFile"
        has_key, mfile = @file_index.shared { |files, _checksums| {files.has_key?(filename), files[filename]?} }
        if has_key
          if mfile
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

      def wait_for_sync(& : -> Nil) : Nil
        @sync_lock.synchronize do
          yield
        end
      end

      def followers : Array(Follower)
        @lock.synchronize do
          @followers.select(&.synced?).dup # for thread safety
        end
      end

      def syncing_followers : Array(Follower)
        @lock.synchronize do
          @followers.select(&.syncing?).dup # for thread safety
        end
      end

      def all_followers : Array(Follower)
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
        # called before accepting followers, no lock needed
        @file_index.lock { |_files, checksums| checksums.restore }
        Log.info { "Listening on #{server.local_address}" }
        @listeners << server

        loop do
          socket = server.accept? || break
          spawn(name: "Clustering follower") { handle_socket(socket) }
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
        @lock.synchronize do
          if stale_follower = @followers.find { |f| f.id == follower.id }
            Log.error { "Disconnecting stale follower with id #{follower.id.to_s(36)}" }
            @followers.delete(stale_follower)
            stale_follower.close
          end
          @followers << follower # Starts in Syncing state
        end
        # Only allow one follower to do full sync at a time
        # The bandwidth between nodes should be very high, so
        # better with one fully synced than 2 partially synced followers
        # @sync_lock is always acquired before @lock to avoid deadlock
        @sync_lock.synchronize do
          follower.full_sync # sync the bulk
          @lock.synchronize do
            follower.full_sync    # sync the last
            follower.mark_synced! # Change state to Synced
            update_isr
          end
        end
        @on_follower_change.try &.call(follower, true)
        begin
          # Wait for follower to disconnect or be closed
          follower.ack_loop
        ensure
          @lock.synchronize do
            @followers.delete(follower)
            @dirty_isr = true
          end
          @on_follower_change.try &.call(follower, false)
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

      def on_follower_change(&blk : Follower, Bool -> Nil) : Nil
        @on_follower_change = blk
      end

      private def update_isr
        isr_key = "#{@config.clustering_etcd_prefix}/isr"
        ids = String.build do |str|
          @followers.each do |f|
            next unless f.synced?
            f.id.to_s(str, 36)
            str << ","
          end
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
        @file_index.lock { |_files, checksums| checksums.store }
      end

      private def each_follower(& : Follower -> Nil) : Nil
        @lock.synchronize do
          update_isr if @dirty_isr
          @followers.each do |f|
            next if f.syncing? # Performing a full sync
            yield f
          rescue IO::Error | Socket::Error
            Log.info { "Follower disconnected address=#{f.remote_address} id=#{f.id.to_s(36)}" }
            Fiber.yield # Allow other fiber to run to remove the follower from array
          end
        end
      end

      private def strip_datadir(path : String) : String
        path[@data_dir.bytesize + 1..]
      end
    end
  end
end
