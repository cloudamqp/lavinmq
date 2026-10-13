require "../clustering"
require "../rate_limiter"
require "../filesystem"
require "./checksums"
require "./proxy"
require "./wire_v3"
require "lz4"
require "http/server"
require "wait_group"

module LavinMQ
  module Clustering
    class Client
      Log = LavinMQ::Log.for "clustering.client"

      # Buffer used when streaming replicated file changes to disk. Matches
      # LZ4::Reader's internal 64 KiB buffer.
      BUFFER_SIZE = 64 * 1024

      # Files #hash_local_files hashes between Fiber.yields; a yield per (often
      # tiny) file costs more than it gives back.
      HASH_YIELD_INTERVAL = 32

      # Capacity of the channel buffering acks from the stream-reading fiber to
      # the ack-sending fiber. Only bounds an in-process queue (send_ack_loop
      # drains and coalesces it continuously), so a fixed size is fine; the
      # leader's ack deadline, not this, governs how far a follower may lag.
      ACK_BUFFER_CAPACITY = 8192

      # A v3 ack in the making: the applied lsn and the absolute received
      # byte count to report
      record Progress, lsn : UInt64, bytes : Int64
      # What the stream-reading fiber hands the ack-sending fiber: v1/v2 byte
      # counts (deltas), v3 progress, and fsync requests (paths, "" for the
      # whole data dir)
      alias Ack = Int64 | String | Progress

      # A file the v3 stream writes to at explicit offsets
      private class OpenFile
        getter fd : Int32
        property size : Int64

        def initialize(@fd : Int32, @size : Int64)
        end

        def close : Nil
          LibC.close(@fd)
        end
      end

      @closed = false
      @closing = Channel(Nil).new # closed by #close, cuts the reconnect wait short
      @amqp_proxy : Proxy?
      @http_proxy : Proxy?
      @mqtt_proxy : Proxy?
      @unix_amqp_proxy : Proxy?
      @unix_http_proxy : Proxy?
      @unix_mqtt_proxy : Proxy?
      @socket : TCPSocket?
      getter streamed_bytes = 0_u64
      # Whether the leader's replication server has let this node in. A
      # leader binds that listener after its client listeners, so it's
      # serving then.
      getter? connected = false
      # Running SHA1 over each file's whole content, adopted as its checksum when
      # tracking ends. nil when we started seeing the file mid-content, so no
      # digest can cover the bytes already on disk (see #digest_for).
      @file_digests = Hash(String, Digest::SHA1?).new
      @follower_done = Channel(Nil).new
      # Buffers acks and fsync requests from the stream-reading fiber to the
      # ack-sending fiber. Replaced with a fresh channel on each (re)connect
      # in #stream_changes.
      @acks = Channel(Ack).new
      # Tracks the ack-sending fiber: #close must wait for it to finish before
      # closing @data_dir_fd, since it may sync (syncfs on that fd) before acks
      # it sends — even acks still buffered in @acks after the stream ends.
      @ack_loops = WaitGroup.new
      # Replication protocol version to connect with; lowered to the leader's
      # when it answers with an older header (see #authenticate). Leaders that
      # only speak version 1 don't send fsync requests, so we sync before
      # every ack then.
      @protocol_version = PROTOCOL_VERSION
      # Set by a downgrade, so the next attempt keeps the lowered version;
      # otherwise every reconnect starts over at ours, which picks up a leader
      # upgraded in place
      @downgraded = false
      # Files this client created whose directory entry isn't fsynced yet,
      # fsynced along with the file when the leader requests it
      @created_files = Set(String).new
      # Files the v3 stream has open, by data-dir relative path
      @open_files = Hash(String, OpenFile).new
      # Lsn of the last record fully applied on this connection (v3)
      @applied_lsn = 0u64
      # Stream bytes received on this connection (v3 acks are absolute)
      @received_bytes = 0i64
      # Connections lost since start, not counting the first connect
      getter reconnects = 0
      # Full syncs done since start; a reconnect that resumes from the
      # journal doesn't count
      getter full_syncs = 0
      # The journal the position below belongs to, 0 for none
      @journal_id = 0u64
      # Highest lsn whose record is durably applied, as last persisted
      @persisted_lsn = 0u64
      @last_persist = Time.instant
      # How often the ack loop syncs the data dir to persist the position,
      # so a restart resumes close to where it was
      PERSIST_INTERVAL = 30.seconds

      def initialize(@config : Config, @id : Int32, @password : String, proxy = true)
        System.maximize_fd_limit
        @data_dir = config.data_dir
        @files = Hash(String, File).new do |h, k|
          path = File.join(@data_dir, k)
          FileSystem.mkdir_p File.dirname(path)
          @created_files << k unless File.exists?(path)
          h[k] = File.open(path, "a").tap &.sync = true
        end
        Dir.mkdir_p @data_dir
        @data_dir_fd = LibC.open(@data_dir.check_no_null_byte, LibC::O_RDONLY)
        raise IO::Error.from_errno("Failed to open #{@data_dir}") if @data_dir_fd < 0
        backup_dir = File.join(@data_dir, "backups")
        FileUtils.rm_rf(backup_dir) if Dir.exists?(backup_dir)
        @checksums = Checksums.new(@data_dir)
        @checksums.restore
        restore_position

        if proxy
          @amqp_proxy = Proxy.new(@config.amqp_bind, @config.amqp_port)
          @http_proxy = Proxy.new(@config.http_bind, @config.http_port)
          @mqtt_proxy = Proxy.new(@config.mqtt_bind, @config.mqtt_port)
          @unix_amqp_proxy = Proxy.new(@config.unix_path) unless @config.unix_path.empty?
          @unix_http_proxy = Proxy.new(@config.http_unix_path) unless @config.http_unix_path.empty?
          @unix_mqtt_proxy = Proxy.new(@config.mqtt_unix_path) unless @config.mqtt_unix_path.empty?
        end
      end

      # Tells whether this node is part of the cluster, to explain why the
      # leader keeps refusing the connection. Set by controllers that have
      # a membership.
      property member_check : Proc(Bool)? = nil

      def follow(uri : String)
        follow(URI.parse(uri))
      end

      def follow(uri : URI)
        host = uri.hostname.not_nil!("Host missing in follow URI")
        port = uri.port || 5679
        follow(host, port)
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def follow(host : String, port : Int32)
        Log.info { "Following #{host}:#{port}" }
        @host = host
        @port = port
        if amqp_proxy = @amqp_proxy
          spawn amqp_proxy.forward_to(host, @config.amqp_port, true), name: "AMQP proxy"
        end
        if http_proxy = @http_proxy
          spawn http_proxy.forward_to(host, @config.http_port), name: "HTTP proxy"
        end
        if mqtt_proxy = @mqtt_proxy
          spawn mqtt_proxy.forward_to(host, @config.mqtt_port, true), name: "MQTT proxy"
        end
        if unix_amqp_proxy = @unix_amqp_proxy
          spawn unix_amqp_proxy.forward_to(host, @config.amqp_port), name: "AMQP proxy"
        end
        if unix_http_proxy = @unix_http_proxy
          spawn unix_http_proxy.forward_to(host, @config.http_port), name: "HTTP proxy"
        end
        if unix_mqtt_proxy = @unix_mqtt_proxy
          spawn unix_mqtt_proxy.forward_to(host, @config.mqtt_port), name: "MQTT proxy"
        end
        loop do
          hash_local_files
          return if @closed
          if @downgraded
            @downgraded = false
          else
            @protocol_version = PROTOCOL_VERSION
          end
          @socket = socket = TCPSocket.new(host, port)
          socket.sync = true
          socket.read_buffering = false # use lz4 buffering
          lz4 = Compress::LZ4::Reader.new(socket)
          sync(socket, lz4)
          Log.info { "Streaming changes" }
          stream_changes(socket, lz4)
        rescue ex : IO::Error
          @connected = false
          lz4.try &.close
          socket.try &.close
          break if @closed
          @reconnects += 1
          if (check = @member_check) && !check.call
            # Refused by the leader, keep retrying: we may be added back, and
            # exiting would only make a supervisor restart us in a loop.
            Log.warn { "Disconnected from server #{host}:#{port} (#{ex}): this node is not a member of the cluster, shut it down" }
            break if closed_while_not_member?(check)
          else
            Log.info { "Disconnected from server #{host}:#{port} (#{ex}), retrying..." }
            break if closed_while_waiting?(1.second)
          end
        end
      ensure
        @follower_done.send(nil)
      end

      # How long a node that isn't a member waits before it's refused again,
      # and both it and the leader log it again
      NOT_MEMBER_RETRY_INTERVAL = 1.minute

      # Waits until this node is added back, or NOT_MEMBER_RETRY_INTERVAL in
      # case it was and doesn't know it yet
      private def closed_while_not_member?(member_check : Proc(Bool)) : Bool
        deadline = Time.instant + NOT_MEMBER_RETRY_INTERVAL
        while Time.instant < deadline
          return true if closed_while_waiting?(1.second)
          break if member_check.call
        end
        false
      end

      private def closed_while_waiting?(span : Time::Span) : Bool
        select
        when @closing.receive?
          true
        when timeout(span)
          false
        end
      end

      def follows?(_nil : Nil) : Bool
        false
      end

      def follows?(uri : String) : Bool
        follows? URI.parse(uri)
      end

      def follows?(uri : URI) : Bool
        host = uri.hostname.not_nil!("Host missing in follow URI")
        port = uri.port || 5679
        follows?(host, port)
      end

      def follows?(host : String, port : Int32) : Bool
        @host == host && @port == port
      end

      private def sync(socket, lz4)
        Log.info { "Connected" }
        authenticate(socket)
        Log.info { "Authenticated" }
        @connected = true
        set_socket_opts(socket)
        if @protocol_version >= 3
          # Offer where we are; the leader resumes the stream from there when
          # it still has every later journal entry
          WireV3.write_resume_request(socket, @journal_id, @persisted_lsn)
          resume, journal_id = WireV3.read_resume_reply(socket)
          if resume
            Log.info { "Resuming replication from journal entry #{@persisted_lsn + 1}" }
            return
          end
          @journal_id = journal_id
        end
        full_sync(socket, lz4)
      end

      private def full_sync(socket, lz4)
        reset_file_state
        # The position is meaningless until the sync is complete; a crash
        # midway must not resume from it
        forget_position
        @full_syncs += 1
        full_sync_time = Time.measure do
          bulk_time = Time.measure { sync_files(socket, lz4) }
          Log.info { "Bulk synchronised in #{bulk_time.total_seconds} seconds" }
          rest_time = Time.measure { sync_files(socket, lz4) }
          Log.info { "Changes since bulk synchronized in #{rest_time.total_seconds} seconds" }
          # Fsync requests only cover what's streamed after this, so make
          # everything received so far durable before acking anything
          sync_to_disk
          if @protocol_version >= 3
            # Everything before the cut is on disk now
            persist_position(WireV3.read_cut(lz4) - 1)
          end
        end
        Log.info { "Fully synchronised in #{full_sync_time.total_seconds} seconds" }
      end

      POSITION_FILE = ".replication_position"

      # The position is only persisted after a sync of the whole data dir,
      # so what it claims applied is on disk; a crash costs at most the
      # entries since, which are re-applied when resuming (records carry
      # their offsets, so applying one twice is harmless).
      private def persist_position(lsn : UInt64) : Nil
        @persisted_lsn = lsn
        @last_persist = Time.instant
        path = File.join(@data_dir, POSITION_FILE)
        tmp = "#{path}.tmp"
        File.write(tmp, "#{@journal_id} #{lsn}\n")
        FileSystem.durable_rename(tmp, path)
      end

      private def forget_position : Nil
        @persisted_lsn = 0u64
        File.delete? File.join(@data_dir, POSITION_FILE)
      end

      private def restore_position : Nil
        journal_id, _, lsn = File.read(File.join(@data_dir, POSITION_FILE)).strip.partition(" ")
        @journal_id = journal_id.to_u64
        @persisted_lsn = lsn.to_u64
      rescue File::NotFoundError
      rescue ex : ArgumentError
        Log.warn { "Ignoring malformed #{POSITION_FILE}: #{ex.message}" }
      end

      # Forget the data dir as the previous connection left it. The sync ahead
      # deletes and re-fetches every file whose hash doesn't match the leader's,
      # which would leave cached handles writing to unlinked inodes and digests
      # covering content that's no longer on disk.
      private def reset_file_state : Nil
        finalize_digests
        @files.each_value &.close
        @files.clear
        @open_files.each_value &.close
        @open_files.clear
        @created_files.clear
      end

      # Adopt the running digests as the files' checksums and stop tracking them.
      private def finalize_digests : Nil
        @file_digests.each do |filename, sha1|
          adopt_digest(filename, sha1)
        end
        @file_digests.clear
      end

      # Store `sha1` as `filename`'s checksum, unless it's untracked (nil, i.e.
      # covers only part of the content) or the file is gone.
      private def adopt_digest(filename : String, sha1 : Digest::SHA1?) : Nil
        return unless sha1
        return unless info = File.info?(File.join(@data_dir, filename))
        @checksums.set(filename, sha1.final, info.size)
      end

      private def set_socket_opts(socket)
        if keepalive = @config.tcp_keepalive
          socket.keepalive = true
          socket.tcp_keepalive_idle = keepalive[0]
          socket.tcp_keepalive_interval = keepalive[1]
          socket.tcp_keepalive_count = keepalive[2]
        end
      end

      private def sync_files(socket, lz4)
        Log.info { "Waiting for list of files" }
        hash_size = Digest::SHA1.new.digest_size

        # Drain the whole file list before hashing anything: stalling between
        # entries can make the leader's file-list write time out and drop us.
        # Once the list is read it waits for our file requests, so the comparison
        # below is free to hash.
        remote_files = Array({String, Bytes}).new
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          break if filename_len.zero?

          filename = lz4.read_string(filename_len)
          remote_hash = Bytes.new(hash_size)
          lz4.read_fully(remote_hash)
          remote_files << {filename, remote_hash}
        end
        Log.info { "Received list of #{remote_files.size} files" }

        # Now compare against local files, with the socket already drained.
        files_to_delete, dirs_to_delete = ls_r(@data_dir)
        requested_files = Array(String).new
        file_count = 0
        files_total = remote_files.size
        Log.info { "Comparing files" }
        log_limiter = RateLimiter.new(2.seconds)
        remote_files.each do |filename, remote_hash|
          path = File.join(@data_dir, filename)
          files_to_delete.delete(path)
          # Walk up the path to remove all ancestors from dirs_to_delete
          dir = File.dirname(path)
          while dirs_to_delete.delete(dir)
            dir = File.dirname(dir)
          end
          if File.exists? path
            # Pre-computed by #hash_local_files, except for files that appeared
            # after that pass.
            unless local_hash = @checksums[filename]?.try &.hash
              local_hash = hash_file(filename, path)
              Fiber.yield # CPU bound, so allow other fibers to run
            end
            if local_hash != remote_hash
              Log.info { "Mismatching hash: #{path}" }
              File.delete path
              requested_files << filename
            else
              Log.debug { "Matching hash: #{path}" }
            end
          else
            requested_files << filename
          end
          file_count &+= 1
          log_limiter.do { Log.info { "Compared #{file_count}/#{files_total} files…" } }
        end
        Log.info { "Compared #{file_count} files, #{requested_files.size} to sync" }
        requested_files.each do |filename|
          request_file(filename, socket)
        end
        end_of_file_list(socket)
        Log.info { "Deleting #{files_to_delete.size} files not on leader" } unless files_to_delete.empty?
        files_to_delete.each do |path|
          Log.debug { "File not on leader: #{path}" }
          File.delete path
          # It got a checksum from #hash_local_files; drop it or the checksum map
          # accumulates dead paths.
          @checksums.delete(relative_path(path))
        rescue ex : File::Error
          Log.warn(exception: ex) { "Failed to delete #{path}" }
        end
        # Clean up any local empty directory
        # Sort and reverse to cleanup longer paths first
        Log.info { "Deleting #{dirs_to_delete.size} directories not on leader" } unless dirs_to_delete.empty?
        dirs_to_delete.sort!.reverse_each do |path|
          if Dir.empty? path
            Log.debug { "Dir empty or missing on leader: #{path}" }
            Dir.delete? path
          else
            Log.warn { "Dir #{path} in delete set, but not empty?" }
          end
        rescue ex : File::Error
          Log.warn(exception: ex) { "Failed to delete #{path}" }
        end
        received_count = 0
        log_limiter = RateLimiter.new(2.seconds)
        requested_files.each do |filename|
          file_from_socket(filename, lz4)
          received_count &+= 1
          log_limiter.do { Log.info { "Received #{received_count}/#{requested_files.size} files" } }
        end
        Log.info { "Received all #{requested_files.size} files" } unless requested_files.empty?
      end

      # Hash every local file before connecting, because hashing while connected
      # holds up the leader, which keeps its sync lock until we answer. Files
      # already in @checksums (from disk or an earlier pass) are skipped.
      private def hash_local_files : Nil
        computed = 0
        files, _dirs = ls_r(@data_dir)
        time = Time.measure do
          Log.info { "Calculating checksums for #{files.size} local files" }
          log_limiter = RateLimiter.new(2.seconds)
          files.each do |path|
            break if @closed
            filename = relative_path(path)
            next if @checksums[filename]?
            hash_file(filename, path)
            computed &+= 1
            Fiber.yield if computed % HASH_YIELD_INTERVAL == 0 # CPU bound, so let other fibers run
            log_limiter.do { Log.info { "Calculated #{computed} checksums" } }
          rescue ex : File::NotFoundError
            Log.debug(exception: ex) { "#{path} disappeared while hashing" }
          rescue ex : File::Error
            # This pass also hashes files the leader doesn't have, so one
            # unreadable file must not wedge the reconnect loop. Left uncached,
            # so the compare loop still fails if the leader has it.
            Log.warn(exception: ex) { "Failed to calculate checksum for #{path}" }
          end
          # #restore truncated checksums.sha1, so snapshot the full set or a
          # crash loses the hashes that were on disk at boot.
          @checksums.store if computed > 0
        end
        Log.info { "Calculated #{computed} checksums (#{files.size} local files) in #{time.total_seconds} seconds" }
      end

      # Hash one local file, persisting the hash right away so progress
      # survives a crash.
      private def hash_file(filename : String, path : String) : Bytes
        Log.debug { "Calculating checksum for #{filename}" }
        size = File.size(path)
        sha1 = Digest::SHA1.new
        sha1.file(path)
        hash = sha1.final
        @checksums.append(filename, hash, size)
        hash
      end

      # Path relative to the data dir, i.e. the name the leader knows a file by.
      private def relative_path(path : String) : String
        path.lchop(@data_dir).lchop('/')
      end

      private def ls_r(dir) : {Array(String), Array(String)}
        files = Array(String).new
        dirs = Array(String).new
        ls_r(dir) do |filename|
          if File.file?(filename)
            files << filename
          elsif File.directory?(filename)
            dirs << filename
          end
        end
        {files, dirs}
      end

      private def ls_r(dir, &blk : String -> Nil)
        Dir.each_child(dir) do |child|
          path = File.join(dir, child)
          if File.directory? path
            yield path
            ls_r(path, &blk)
          else
            # Local-only files the leader never sends; skip them so the
            # "delete files not on leader" sweep doesn't wipe them
            next if child.in?(NODE_LOCAL_FILES) || child.in?("checksums.sha1", "checksums.sha1.tmp")
            yield path
          end
        end
      end

      private def request_file(filename, socket)
        Log.debug { "Requesting #{filename}" }
        socket.write_bytes filename.bytesize, IO::ByteFormat::LittleEndian
        socket.write filename.to_slice
      end

      private def end_of_file_list(socket)
        socket.write_bytes 0 # endian-agnostic
      end

      private def file_from_socket(filename, lz4)
        Log.debug { "Waiting for #{filename}" }
        path = File.join(@data_dir, filename)
        FileSystem.mkdir_p File.dirname(path)
        length = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        Log.debug { "Receiving #{filename}, #{length.humanize_bytes}" }
        File.open(path, "w") do |f|
          buffer = uninitialized UInt8[BUFFER_SIZE]
          remaining = length
          sha1 = Digest::SHA1.new
          while (len = lz4.read(buffer.to_slice[0, Math.min(buffer.size, Math.max(remaining, 0))])) > 0
            bytes = buffer.to_slice[0, len]
            f.write bytes
            sha1.update bytes
            remaining &-= len
          end
          remaining.zero? || raise IO::EOFError.new
          # Persist immediately too: a file received here is complete and
          # stable, so a crash mid-sync won't force re-hashing it on restart.
          @checksums.append(filename, sha1.final, length)
        end
        Log.debug { "Received #{filename}, #{length.humanize_bytes}" }
      end

      private def stream_changes(socket, lz4)
        acks = @acks = Channel(Ack).new(ACK_BUFFER_CAPACITY)
        @ack_loops.spawn(name: "Send ack loop") { send_ack_loop(acks, socket) }
        # Stops the logging fiber when this stream ends, so a reconnect doesn't
        # leave one behind per connection (they all report the same counter).
        log_loop_done = Channel(Nil).new
        spawn log_streamed_bytes_loop(log_loop_done), name: "Log streamed bytes loop"
        if @protocol_version >= 3
          stream_changes_v3(lz4)
        else
          stream_changes_v2(lz4)
        end
      ensure
        @acks.close
        log_loop_done.try &.close
      end

      # Version 3: every record names its journal entry and an append its
      # offset (see WireV3). Records are applied in place, so a retransmitted
      # one is harmless, and acked by lsn once applied.
      private def stream_changes_v3(lz4)
        @applied_lsn = @persisted_lsn
        @received_bytes = 0i64
        # The leader streams from the entry after the one we reported (after a
        # full sync: the cut), so the first lsn is known
        expected = (@persisted_lsn + 1).as(UInt64?)
        loop do
          header = WireV3.read_header(lz4)
          if expected && header.lsn != expected
            raise ProtocolError.new("Expected journal entry #{expected}, got #{header.lsn}")
          end
          expected = header.lsn + 1
          case header.op
          in .append?  then apply_append(header, lz4)
          in .replace? then apply_replace(header, lz4)
          in .delete?
            delete(header.path)
            applied(header.lsn, WireV3.record_size(header.op, header.path))
          in .fsync?
            fsync_request(header.path)
            # Reported after the fsync: the ack loop syncs the paths queued
            # before it sends this progress
            applied(header.lsn, WireV3.record_size(header.op, header.path))
          end
        end
      end

      # Count `bytes` of a record as received and report them with the lsn
      # of the last record fully applied, so the leader sees progress inside
      # a large record without taking it as applied
      private def received(bytes : Int) : Nil
        @received_bytes += bytes
        @streamed_bytes &+= bytes
        @acks.send Progress.new(@applied_lsn, @received_bytes)
      end

      # The record `lsn` is applied, along with its last `bytes`
      private def applied(lsn : UInt64, bytes : Int) : Nil
        @applied_lsn = lsn
        received(bytes)
      end

      private def apply_append(header : WireV3::Header, lz4 : IO) : Nil
        offset = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        length = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        raise ProtocolError.new("Invalid append offset #{offset} length #{length}") if offset < 0 || length < 0
        filename = header.path
        Log.debug { "Appending #{length} bytes to #{filename} at #{offset}" }
        file = open_file(filename)
        if offset > file.size
          raise ProtocolError.new("#{filename}: append at #{offset} beyond its #{file.size} bytes (journal entry #{header.lsn})")
        end
        received(WireV3.record_size(header.op, filename)) # the framing
        mismatch = write_at(file, lz4, offset, length, digest_for(filename, file.size), header.lsn)
        applied(header.lsn, 0) if length.zero?
        if mismatch
          Log.warn { "#{filename}: #{length} bytes at #{offset} differ from the #{file.size} already on disk (journal entry #{header.lsn}), dropping its checksum" }
          @checksums.delete(filename)
          @file_digests[filename] = nil
        end
      end

      # Stream `length` bytes from lz4 into the file at `offset`, reporting
      # progress per chunk and the record as applied with the last one.
      # Returns whether bytes already on disk differed from the stream.
      private def write_at(file : OpenFile, lz4 : IO, offset : Int64, length : Int64, digest : Digest::SHA1?, lsn : UInt64) : Bool
        buffer = uninitialized UInt8[BUFFER_SIZE]
        existing = uninitialized UInt8[BUFFER_SIZE]
        pos = offset
        remaining = length
        mismatch = false
        while remaining > 0
          len = lz4.read(buffer.to_slice[0, Math.min(buffer.size, remaining)])
          raise IO::EOFError.new if len.zero?
          chunk = buffer.to_slice[0, len]
          # Bytes below the file's size were already received (the join cut's
          # tail, or a file a compaction shrank after the leader snapshotted
          # its size); they're expected to be identical
          overlap = Math.min(file.size - pos, len.to_i64).to_i32
          if overlap > 0
            pread_all(file.fd, existing.to_slice[0, overlap], pos)
            mismatch = true unless chunk[0, overlap] == existing.to_slice[0, overlap]
          end
          pwrite_all(file.fd, chunk, pos)
          # The digest already covers the overlapping bytes
          digest.try &.update(chunk[overlap..]) if !mismatch && len > overlap
          pos += len
          remaining -= len
          file.size = pos if pos > file.size
          remaining.zero? ? applied(lsn, len) : received(len)
        end
        mismatch
      end

      private def apply_replace(header : WireV3::Header, lz4 : IO) : Nil
        length = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
        raise ProtocolError.new("Invalid replace length #{length}") if length < 0
        filename = header.path
        Log.debug { "Replacing file #{filename} (#{length} bytes)" }
        @files.delete(filename).try &.close
        @open_files.delete(filename).try &.close
        received(WireV3.record_size(header.op, filename)) # the framing

        # A replace rewrites the file from byte 0, so the digest below covers its
        # whole content — but only once the rename installs it. Until then the old
        # file is on disk, so leave it with a checksum matching it and start
        # tracking the new digest only after the rename.
        adopt_digest(filename, @file_digests.delete(filename))
        sha1 = Digest::SHA1.new

        path = File.join(@data_dir, "#{filename}.tmp")
        FileSystem.mkdir_p File.dirname(path)
        @created_files.delete(filename)
        File.open(path, "w") do |f|
          f.sync = true
          buffer = uninitialized UInt8[BUFFER_SIZE]
          remaining = length
          last = 0
          while remaining > 0
            len = lz4.read(buffer.to_slice[0, Math.min(buffer.size, remaining)])
            raise IO::EOFError.new if len.zero?
            bytes = buffer.to_slice[0, len]
            f.write(bytes)
            sha1.update(bytes)
            remaining -= len
            # The last chunk is reported once the rename has installed the
            # file, so the record's ack means it's applied
            remaining.zero? ? (last = len) : received(len)
          end
          # Fsync the content before the rename, so that a crash can't leave
          # a partially written file under the final name
          f.fsync if @config.sync?
          f.rename f.path[0..-5]
          @file_digests[filename] = sha1
          @acks.send File.dirname(f.path)
          applied(header.lsn, last)
        end
      end

      # Opens (creating if missing) a file the v3 stream writes at offsets.
      # O_RDWR rather than append mode: pwrite ignores the offset on an
      # O_APPEND descriptor.
      private def open_file(filename : String) : OpenFile
        @open_files[filename] ||= begin
          path = File.join(@data_dir, filename).check_no_null_byte
          FileSystem.mkdir_p File.dirname(path)
          perms = 0o644
          fd = LibC.open(path, LibC::O_RDWR | LibC::O_CREAT | LibC::O_EXCL, perms)
          if fd >= 0
            @created_files << filename
          elsif Errno.value == Errno::EEXIST
            fd = LibC.open(path, LibC::O_RDWR | LibC::O_CREAT, perms)
          end
          raise File::Error.from_errno("Error opening file", file: path) if fd < 0
          if LibC.fstat(fd, out stat) < 0
            err = File::Error.from_errno("Unable to get info", file: path)
            LibC.close(fd)
            raise err
          end
          OpenFile.new(fd, stat.st_size.to_i64)
        end
      end

      private def pwrite_all(fd : Int32, bytes : Bytes, offset : Int64) : Nil
        until bytes.empty?
          n = LibC.pwrite(fd, bytes.to_unsafe.as(Void*), bytes.size, offset)
          if n < 0
            next if Errno.value == Errno::EINTR
            raise IO::Error.from_errno("pwrite")
          end
          bytes += n
          offset += n
        end
      end

      private def pread_all(fd : Int32, bytes : Bytes, offset : Int64) : Nil
        until bytes.empty?
          n = LibC.pread(fd, bytes.to_unsafe.as(Void*), bytes.size, offset)
          if n < 0
            next if Errno.value == Errno::EINTR
            raise IO::Error.from_errno("pread")
          end
          raise IO::EOFError.new if n.zero?
          bytes += n
          offset += n
        end
      end

      # Versions 1 and 2: a record is [i32 path_len][path][i64 len][payload];
      # a negative len appends (at the end, no offset), zero deletes (or is an
      # fsync request when the path starts with '$'), positive replaces.
      private def stream_changes_v2(lz4)
        loop do
          filename_len = lz4.read_bytes Int32, IO::ByteFormat::LittleEndian
          next if filename_len.zero?
          filename = lz4.read_string(filename_len)

          len = lz4.read_bytes Int64, IO::ByteFormat::LittleEndian
          # For append/replace the framing bytes (length headers + filename)
          # are acked up front and the payload is acked incrementally as it's
          # written (see stream_with_checksum), so a single large action keeps
          # the leader's progress deadline reset instead of going silent until
          # it's done. For a delete the framing is the entire record — acking
          # it tells the leader the deletion is durable — so it's only acked
          # once the deletion has been applied.
          framing = sizeof(Int32) + filename_len + sizeof(Int64)
          if len.zero? && filename.starts_with?(FSYNC_PREFIX)
            fsync_request(filename[1..])
            ack(framing)
            next
          end
          case len
          when .negative? # append bytes to file
            ack(framing)
            append(filename, len, lz4)
          when .zero? # file is deleted
            delete(filename)
            ack(framing)
          when .positive? # replace file
            ack(framing)
            replace(filename, len, lz4)
          end
        end
      end

      private def append(filename, len, lz4)
        Log.debug { "Appending #{len.abs} bytes to #{filename}" }
        f = @files[filename]
        stream_with_checksum(lz4, f, len.abs, digest_for(filename, f))
      end

      # The running digest over `filename`'s whole content, or nil if we can't
      # keep one: Digest::SHA1 can't be seeded with a hash, so tracking can only
      # start while the file is empty (`file` is opened in append mode, so its
      # size here is still the pre-append size). If it isn't, the checksum we
      # hold goes stale with this append and is dropped, making the next sync
      # re-hash the file from disk.
      private def digest_for(filename : String, file : File) : Digest::SHA1?
        digest_for(filename, file.size)
      end

      private def digest_for(filename : String, size : Int) : Digest::SHA1?
        # #fetch, not #[]?, to tell an absent entry from an untracked file
        @file_digests.fetch(filename) do
          if size.zero?
            @file_digests[filename] = Digest::SHA1.new
          else
            Log.debug { "Not checksumming #{filename}, appending to existing content" }
            @checksums.delete(filename)
            @file_digests[filename] = nil
          end
        end
      end

      # Queue the fsync for the ack-sending fiber, which syncs before it sends
      # the ack for this request. An empty filename means the whole data dir.
      private def fsync_request(filename : String) : Nil
        Log.debug { "Fsync requested for #{filename.empty? ? "data dir" : filename}" }
        if filename.empty?
          @acks.send ""
          return
        end
        path = File.join(@data_dir, filename)
        @acks.send path
        @acks.send File.dirname(path) if @created_files.delete(filename)
      end

      private def delete(filename)
        Log.debug { "Deleting #{filename}" }
        @created_files.delete(filename)
        @files.delete(filename).try &.close
        @open_files.delete(filename).try &.close
        File.delete? File.join(@data_dir, filename)
        @checksums.delete(filename)
        @file_digests.delete(filename)
        delete_empty_dirs File.dirname(filename)
      end

      # Removes now-empty parent directories (e.g. an emptied queue dir) after a
      # file delete. The leader only streams file deletes, not directory deletes,
      # so without this empty queue dirs would linger until the next full sync.
      #
      # We walk up one level per iteration until File.dirname reaches ".". The
      # non-recursive Dir.delete raises File::Error if the dir still has files
      # (or is already gone), and the rescue stops the walk safely. Both append
      # and replace file re-create the full path if needed.
      private def delete_empty_dirs(dir)
        while dir != "."
          path = File.join(@data_dir, dir)
          rmdir(path) || break
          Log.debug { "Deleted empty dir #{dir}" }
          dir = File.dirname(dir)
        end
      rescue ex : File::Error
        Log.error(exception: ex) { "Could not delete #{dir}: #{ex.message}" }
      end

      # rmdir returns false if the dir isn't empty, true if it was removed, and raises on other errors (e.g. permissions).
      private def rmdir(path)
        if LibC.rmdir(path.check_no_null_byte) == 0
          true
        elsif Errno.value.in?(Errno::ENOTEMPTY, Errno::EEXIST, Errno::ENOENT)
          false
        else
          raise ::File::Error.from_errno("Unable to remove directory", file: path)
        end
      end

      private def replace(filename, len, lz4)
        Log.debug { "Replacing file #{filename} (#{len} bytes)" }
        @files.delete(filename).try &.close

        # A replace rewrites the file from byte 0, so the digest below covers its
        # whole content — but only once the rename installs it. Until then the old
        # file is on disk, so leave it with a checksum matching it and start
        # tracking the new digest only after the rename.
        adopt_digest(filename, @file_digests.delete(filename))
        sha1 = Digest::SHA1.new

        path = File.join(@data_dir, "#{filename}.tmp")
        FileSystem.mkdir_p File.dirname(path)
        @created_files.delete(filename)
        File.open(path, "w") do |f|
          f.sync = true
          # The record's final ack tells the leader the replace is durable, so
          # it must not be sent while the new content only exists as the .tmp
          # file; hold it back until the rename has installed the file.
          deferred = stream_with_checksum(lz4, f, len, sha1, defer_final_ack: true)
          # Fsync the content before the rename, so that a crash can't leave
          # a partially written file under the final name
          f.fsync if @config.sync?
          f.rename f.path[0..-5]
          @file_digests[filename] = sha1
          @acks.send File.dirname(f.path)
          ack(deferred)
        end
      end

      # Read from lz4, update SHA1, and write to file incrementally.
      # Returns the number of bytes received but not yet acked (see below).
      # A nil `sha1` streams the bytes without hashing them (see #digest_for).
      private def stream_with_checksum(lz4 : IO, file : IO, length : Int64, sha1 : Digest::SHA1?, defer_final_ack = false) : Int64
        # Read, hash, and write incrementally. Each chunk is acked as soon as
        # it's persisted so the leader sees continuous progress within a large
        # action and won't evict us on its ack deadline (a 128 MiB message would
        # otherwise stream for >10s with no ack on a 100 Mbit/s link).
        # With defer_final_ack the last chunk is not acked but its size
        # returned, for callers that must apply the action (replace's rename)
        # before the leader may consider it durable.
        buffer = uninitialized UInt8[BUFFER_SIZE]
        remaining = length
        while remaining > 0
          len = lz4.read(buffer.to_slice[0, Math.min(buffer.size, remaining)])
          raise IO::EOFError.new if len.zero?
          bytes = buffer.to_slice[0, len]
          file.write(bytes)
          sha1.try &.update(bytes)
          remaining -= len
          return len.to_i64 if remaining.zero? && defer_final_ack
          ack(len)
        end
        0i64
      end

      # Count streamed bytes and forward the count to the ack-sending fiber.
      private def ack(bytes : Int) : Nil
        n = bytes.to_i64
        @streamed_bytes &+= n
        @acks.send(n)
      end

      # Concatenate as many acks as possible to generate few TCP packets.
      # The leader holds publish confirms until in-sync followers have acked,
      # so the files it requests fsyncs for must be durable here before the
      # acks that follow the requests are sent (unless sync is disabled).
      # Syncing once per coalesced batch makes batching emerge naturally —
      # acks and requests accumulate while the blocking syncs run.
      private def send_ack_loop(acks, socket)
        socket.tcp_nodelay = true
        fsync_paths = Set(String).new
        while item = acks.receive?
          ack_bytes = 0i64 # v1/v2: bytes to ack, a delta
          progress = nil.as(Progress?)
          syncfs = @protocol_version < 2
          loop do
            case item
            in Int64    then ack_bytes += item
            in Progress then progress = item # absolute, the latest wins
            in String   then item.empty? ? (syncfs = true) : fsync_paths << item
            end
            item = acks.try_receive? || break
          end
          synced = sync_batch(syncfs, fsync_paths)
          fsync_paths.clear
          if progress
            persist_if_due(progress.lsn, synced)
            WireV3.write_ack(socket, progress.lsn, progress.bytes)
          elsif ack_bytes > 0
            socket.write_bytes ack_bytes, IO::ByteFormat::LittleEndian
          end
        end
      rescue Channel::ClosedError
      rescue IO::Error
        socket.close rescue nil
      end

      # Sync what a batch of acks requires: the requested paths, or the whole
      # data dir when asked to or when they are many. Returns whether the
      # whole data dir was synced.
      private def sync_batch(syncfs : Bool, paths : Set(String)) : Bool
        if syncfs || paths.size > @config.syncfs_threshold
          sync_to_disk
          true
        else
          fsync_to_disk(paths)
          false
        end
      end

      # Everything applied up to `lsn` is on disk after a sync of the whole
      # data dir, so the position can move there; done at every such sync
      # and at least every PERSIST_INTERVAL while entries are applied
      private def persist_if_due(lsn : UInt64, synced : Bool) : Nil
        return unless synced || (lsn > @persisted_lsn && Time.instant - @last_persist > PERSIST_INTERVAL)
        sync_to_disk unless synced
        persist_position(lsn)
      end

      # Make all replicated writes durable before acking the leader.
      private def sync_to_disk : Nil
        return unless @config.sync?
        sync_data_dir
      rescue ex
        # Can't ack data that isn't durable; die fast so the leader drops us
        # from the in-sync set and stops confirming publishes on our acks.
        Log.fatal(exception: ex) { "Failed to sync: #{ex.message}" }
        exit 1
      end

      private def sync_data_dir : Nil
        FileSystem.syncfs(@data_dir_fd)
      end

      private def fsync_to_disk(paths : Set(String)) : Nil
        return unless @config.sync?
        paths.each { |path| fsync_path(path) }
      rescue ex
        Log.fatal(exception: ex) { "Failed to fsync: #{ex.message}" }
        exit 1
      end

      # A path deleted since it was requested needs no fsync
      private def fsync_path(path : String) : Nil
        File.open(path, &.fsync)
      rescue File::NotFoundError
      end

      # Logs the streamed byte count until #stream_changes closes the done
      # channel (or the client is closed), so the fiber doesn't outlive the
      # stream that spawned it.
      private def log_streamed_bytes_loop(done : Channel(Nil))
        loop do
          select
          when done.receive?
            break
          when timeout(30.seconds)
            break if @closed
            Log.info { "Total streamed bytes: #{@streamed_bytes}" }
          end
        end
      end

      private def authenticate(socket)
        header = start_header
        socket.write header
        if @protocol_version < 2
          socket.write_bytes @password.bytesize.to_u8, IO::ByteFormat::LittleEndian
          socket.write @password.to_slice
        else
          answer_challenge(socket, header)
        end
        case socket.read_byte
        when 0 # ok
        when 1   then raise AuthenticationError.new
        when nil then raise IO::EOFError.new
        else          raise ProtocolError.new("Unknown response from authentication")
        end
        socket.write_bytes @id, IO::ByteFormat::LittleEndian
      end

      private def start_header : Bytes
        case @protocol_version
        when 1 then Start
        when 2 then StartV2
        else        StartV3
        end
      end

      # A leader that speaks our version echoes the header followed by the
      # challenge; one that doesn't answers with the newest header it speaks,
      # which we reconnect with.
      private def answer_challenge(socket, header : Bytes)
        echo = Bytes.new(header.size)
        socket.read_fully(echo)
        unless echo == header
          theirs = Clustering.header_version(echo)
          if theirs == 1 && @config.clustering_backend.raft?
            # Raft clusters have no version 1 nodes, so a version 1 "leader"
            # would be someone trying to get the password in clear text
            Log.error { "Leader only supports replication protocol version 1, refusing to send it the password" }
            raise IO::Error.new("Replication protocol version mismatch")
          end
          if 1 <= theirs < @protocol_version
            Log.warn { "Leader only supports replication protocol version #{theirs}, reconnecting with it" }
            @protocol_version = theirs
            @downgraded = true
            raise IO::Error.new("Replication protocol version mismatch")
          end
          raise ProtocolError.new("Unsupported replication protocol: #{String.new(echo).inspect}")
        end
        challenge = Bytes.new(CHALLENGE_SIZE)
        socket.read_fully(challenge)
        socket.write Clustering.challenge_response(@password, challenge, header)
      end

      def close
        return if @closed
        @closed = true
        @closing.close
        @amqp_proxy.try &.close
        @http_proxy.try &.close
        @mqtt_proxy.try &.close
        @unix_amqp_proxy.try &.close
        @unix_http_proxy.try &.close
        @unix_mqtt_proxy.try &.close
        @files.each_value &.close
        @open_files.each_value &.close
        @socket.try &.close
        # Wait for follower loop to exit (with timeout to prevent hanging)
        select
        when @follower_done.receive
        when timeout(5.seconds)
          Log.warn { "Follower loop did not exit within timeout, forcing shutdown" }
        end
        # The ack loop keeps draining acks buffered in @acks even after the
        # channel is closed, syncing to disk before each send. Wait for it to
        # finish before closing @data_dir_fd below, or its syncfs would hit a
        # closed (or worse, reused) fd and the process would exit 1 mid
        # shutdown/promotion. Closing @acks is normally done by stream_changes,
        # but do it here too in case the follower loop is stuck.
        @acks.close
        @ack_loops.wait
        # Finalize all pending checksums
        finalize_digests
        @checksums.store
        # Record where to resume from; a promotion or restart then needs no
        # full sync. The ack loop is done, so the applied lsn is final.
        if @protocol_version >= 3 && @applied_lsn > @persisted_lsn
          begin
            sync_data_dir if @config.sync?
            persist_position(@applied_lsn)
          rescue ex
            Log.warn(exception: ex) { "Could not persist the replication position" }
          end
        end
        LibC.close(@data_dir_fd) if @data_dir_fd >= 0
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
