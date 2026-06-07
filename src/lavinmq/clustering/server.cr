require "../clustering"
require "./file_index"
require "./replicator"
require "./follower"
require "./checksums"
require "./coordinator"
require "../config"
require "../message"
require "../mfile"
require "crypto/subtle"
require "lz4"
require "sync/shared"

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

      def initialize(config : Config, @coordinator : Coordinator, @id : Int32)
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
        offset = pos.to_i64
        each_follower { |f| f.append(path, bytes) unless f.replayed?(path, offset) }
      end

      # Replicate a small in-memory integer (e.g. Schema::VERSION, ack
      # position). The follower writes it directly to its LZ4 stream — no
      # heap allocation. `offset` is where the value is written on the leader,
      # used to skip bytes a just-joined follower already got via full_sync.
      def append_value(path : String, value : UInt32 | Int32, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower { |f| f.append(path, value) unless f.replayed?(path, offset) }
      end

      # Replicate an in-memory byte buffer. Caller owns the buffer. See
      # append_value for `offset`.
      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        path = strip_datadir path
        @file_index.lock { |_files, checksums| checksums.delete(path) }
        each_follower { |f| f.append(path, bytes) unless f.replayed?(path, offset) }
      end

      def delete_file(path : String)
        path = strip_datadir path
        @file_index.lock do |files, checksums|
          files.delete(path)
          checksums.delete(path)
        end
        each_follower do |f|
          f.delete(path)
          f.forget_baseline(path) # path may be reused by a future file
        end
      end

      def nr_of_files
        @file_index.shared { |files, _checksums| files.size }
      end

      # When `caps` is given (the last full_sync of a joining follower), each
      # file's hash is computed over only the first `caps[path]` bytes — the
      # cut the follower will be marked synced at. This excludes writes whose
      # replication dispatch hasn't happened yet (local write at
      # message_store.cr:339 races full_sync because it doesn't hold @lock), so
      # they are delivered incrementally instead of duplicated. Files absent
      # from `caps` are capped at 0 (delivered entirely via the stream).
      def files_with_hash(caps : Hash(String, Int64)? = nil, & : Tuple(String, Bytes) -> Nil)
        # Snapshot the index to allow safe iteration without holding the lock.
        # Files are read from disk via File handles (no MFile mmap reads), so
        # concurrent close of the MFile is harmless. For sparse MFile-backed
        # files, we cap the hash at mfile.size to match the follower's
        # contiguous (non-sparse) copy.
        snapshot = @file_index.shared { |files, _checksums| files.dup }
        sha1 = Digest::SHA1.new
        snapshot.each do |path, mfile|
          # The cache holds full-size hashes; a capped pass must recompute.
          cached_hash = caps ? nil : @file_index.shared { |_files, checksums| checksums[path]? }
          if cached_hash
            yield({path, cached_hash})
          else
            filename = File.join(@data_dir, path)
            begin
              File.open(filename) do |f|
                size = mfile ? mfile.size : f.size.to_i64
                size = Math.min(size, caps[path]? || 0i64) if caps
                sha1.update IO::Sized.new(f, size)
              end
              hash = sha1.final
              sha1.reset
              @file_index.lock { |_files, checksums| checksums[path] = hash } unless caps
              yield({path, hash})
            rescue File::NotFoundError
              next # File disappeared since we took the snapshot, just skip it.
            end
          end
        end
      end

      # Yields the file (or nil if missing) along with its real data size in
      # bytes. For MFile-backed sparse files the size comes from mfile.size,
      # not from File.size which would be the capacity. `cap` (see
      # files_with_hash) limits the yielded size for a capped full_sync.
      def with_file(filename, cap : Int64? = nil, & : File?, Int64 -> _)
        # has_key? needed because a registered path with a nil MFile is
        # different from a path that's not in the index at all.
        has_key, mfile = @file_index.shared { |files, _checksums| {files.has_key?(filename), files[filename]?} }
        if has_key
          path = File.join(@data_dir, filename)
          begin
            File.open(path) do |f|
              f.read_buffering = false
              size = mfile ? mfile.size : f.size.to_i64
              size = Math.min(size, cap) if cap
              yield f, size
            end
          rescue File::NotFoundError
            yield nil, 0i64
          end
        else
          yield nil, 0i64
        end
      end

      # Snapshot the current logical size of every indexed file, using the same
      # size rule as full_sync (mfile.size for sparse MFiles, else File.size).
      # Captured at mark_synced to record the per-file cut a joining follower
      # received via full_sync. Callers hold @lock.
      private def snapshot_sizes : Hash(String, Int64)
        sizes = Hash(String, Int64).new
        @file_index.shared do |files, _checksums|
          files.each do |path, mfile|
            if mfile
              sizes[path] = mfile.size.to_i64
            else
              begin
                sizes[path] = File.size(File.join(@data_dir, path)).to_i64
              rescue File::NotFoundError
                next
              end
            end
          end
        end
        sizes
      end

      def followers : Array(Follower)
        @lock.synchronize do
          @followers.select(&.synced?) # for thread safety
        end
      end

      def syncing_followers : Array(Follower)
        @lock.synchronize do
          @followers.select(&.syncing?) # for thread safety
        end
      end

      def all_followers : Array(Follower)
        @lock.synchronize do
          @followers.dup # for thread safety
        end
      end

      def password : String
        @coordinator.password
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
            # Capture the per-file cut BEFORE the last sync and cap the sync to
            # it, so what full_sync sends equals the baseline exactly. A larger
            # mfile.size from an in-flight write (local write done, replication
            # dispatch still pending) is excluded here and delivered via the
            # stream instead of being duplicated.
            cut = snapshot_sizes
            follower.full_sync(cut) # sync the last, capped at the cut
            follower.capture_synced_baseline(cut)
            follower.mark_synced! # Change state to Synced
            update_isr
          end
        end
        begin
          # Wait for follower to disconnect or be closed
          follower.ack_loop
        ensure
          @lock.synchronize do
            # If the follower was behind (unacked replicated data) when it
            # dropped, it may be missing data that's about to be confirmed via
            # the surviving followers, so it must leave the etcd ISR now rather
            # than on the next replication write — otherwise it could be promoted
            # on failover lacking already-confirmed data. A caught-up follower
            # (no lag) still has everything confirmed so far, so we leave it in
            # the ISR as a valid failover candidate; the next write's each_follower
            # flush removes it before anything it lacks is confirmed.
            behind = follower.lag_in_bytes > 0
            @followers.delete(follower)
            @dirty_isr = true
            if behind
              begin
                update_isr # @dirty_isr stays set, so the lazy path retries on failure
              rescue ex
                Log.warn(exception: ex) { "Failed to update ISR after follower id=#{follower.id.to_s(36)} disconnected" }
              end
            end
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
        ids = Set(Int32).new
        @followers.each do |f|
          ids.add(f.id) if f.synced?
        end
        ids.add(@id)
        Log.info { "In-sync replicas: #{ids.to_a}" }
        @coordinator.update_isr(ids)
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
