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
      # Maps relative paths to their MFile (for sparse, mmap-backed files) or
      # nil (for regular files where File.size is authoritative). MFile-backed
      # segments are sparse: ftruncate'd to capacity then appended from the
      # beginning, so we need mfile.size to know the real data length and
      # mfile.to_slice to read from the mmap. Full-sync paths always read from
      # disk via fresh File handles; only the append hot path reads the mmap.
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
        register_file file.path
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

      # Replicate `length` bytes from `path` starting at `pos`. The bytes are
      # read from the registered MFile's mmap — zero syscalls. Callers must
      # have registered the path as an MFile first; for regular Files use the
      # `append(path, bytes)` overload instead.
      def append(path : String, pos : Int, length : Int)
        path = strip_datadir path
        mfile = @file_index.lock do |files, checksums|
          checksums.delete(path)
          files[path]?
        end
        raise ArgumentError.new("append(pos, length) requires an MFile-registered path: #{path}") unless mfile
        bytes = mfile.to_slice(pos.to_i64, length.to_i64)
        each_follower &.append(path, bytes)
      end

      # Replicate a small in-memory integer (e.g. Schema::VERSION, ack
      # position). The follower writes it directly to its LZ4 stream — no
      # heap allocation.
      def append(path : String, value : UInt32 | Int32)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower &.append(path, value)
      end

      # Replicate an in-memory byte buffer. Caller owns the buffer.
      def append(path : String, bytes : Bytes)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower &.append(path, bytes)
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
        # Snapshot the index to allow safe iteration without holding the lock.
        # Files are read from disk via File handles (no MFile mmap reads), so
        # concurrent close of the MFile is harmless. For sparse MFile-backed
        # files, we cap the hash at mfile.size to match the follower's
        # contiguous (non-sparse) copy.
        snapshot = @file_index.shared { |files, _checksums| files.dup }
        sha1 = Digest::SHA1.new
        snapshot.each do |path, mfile|
          cached_hash = @file_index.shared { |_files, checksums| checksums[path]? }
          if cached_hash
            yield({path, cached_hash})
          else
            filename = File.join(@data_dir, path)
            begin
              File.open(filename) do |f|
                size = mfile ? mfile.size : f.size.to_i64
                sha1.update IO::Sized.new(f, size)
              end
              hash = sha1.final
              sha1.reset
              @file_index.lock { |_files, checksums| checksums[path] = hash }
              yield({path, hash})
            rescue File::NotFoundError
              next # File disappeared since we took the snapshot, just skip it.
            end
          end
        end
      end

      # Yields the file (or nil if missing) along with its real data size in
      # bytes. For MFile-backed sparse files the size comes from mfile.size,
      # not from File.size which would be the capacity.
      def with_file(filename, & : File?, Int64 -> _)
        # has_key? needed because a registered path with a nil MFile is
        # different from a path that's not in the index at all.
        has_key, mfile = @file_index.shared { |files, _checksums| {files.has_key?(filename), files[filename]?} }
        if has_key
          path = File.join(@data_dir, filename)
          begin
            File.open(path) do |f|
              f.read_buffering = false
              size = mfile ? mfile.size : f.size.to_i64
              yield f, size
            end
          rescue File::NotFoundError
            yield nil, 0i64
          end
        else
          yield nil, 0i64
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
        begin
          # Wait for follower to disconnect or be closed
          follower.ack_loop
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
