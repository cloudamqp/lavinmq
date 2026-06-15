require "../../config"

module LavinMQ
  module Clustering
    module VR
      # Defined here too (reopened) so this file is usable without membership.cr.
      class Error < Exception; end

      # Crash-safe, fsync'd persistent state for the VR protocol, stored at
      # `data_dir/.vr_state`. Three monotonic values:
      #
      # * `view`      — the current view number. Must be durable BEFORE a node
      #                 sends DOVIEWCHANGE/STARTVIEW or serves as primary in it,
      #                 so a crash can't make it accept two views as normal. This
      #                 is the fencing that the etcd lease used to provide.
      # * `op`        — the highest op-number this node has durably applied.
      # * `commit_op` — the highest op-number known committed (a safe lower bound;
      #                 under-reporting is safe, just slower).
      #
      # The file is rewritten atomically (temp + rename + directory fsync). A
      # write failure on a clustered node is fatal: we exit 1 so the node leaves
      # the cluster rather than continue with un-durable consensus state, matching
      # the fsync-failure policy elsewhere in clustering (client.cr, persister.cr).
      class State
        Log = LavinMQ::Log.for "clustering.vr.state"

        MAGIC    = Bytes['V'.ord, 'R'.ord, 'S'.ord, 'T'.ord]
        VERSION  = 1u8
        FILENAME = ".vr_state"
        # magic(4) + version(1) + view(8) + op(8) + commit_op(8)
        SIZE = 4 + 1 + 8 + 8 + 8

        getter view : UInt64
        getter op : UInt64
        getter commit_op : UInt64

        def initialize(@data_dir : String, @view : UInt64 = 0, @op : UInt64 = 0, @commit_op : UInt64 = 0)
        end

        # Load the persisted state, or a fresh zeroed state if the file is absent
        # (a node that has never participated). A present-but-corrupt file raises
        # VR::Error — it must not be silently reset, as that would discard the
        # fencing view number.
        def self.load(data_dir : String) : State
          path = File.join(data_dir, FILENAME)
          File.open(path) do |f|
            magic = Bytes.new(MAGIC.size)
            f.read_fully(magic)
            raise Error.new("Corrupt #{FILENAME}: bad magic") unless magic == MAGIC
            version = f.read_bytes(UInt8)
            raise Error.new("Unsupported #{FILENAME} version #{version}") unless version == VERSION
            view = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
            op = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
            commit_op = f.read_bytes(UInt64, IO::ByteFormat::LittleEndian)
            new(data_dir, view, op, commit_op)
          end
        rescue File::NotFoundError
          new(data_dir)
        end

        @lock = Mutex.new(:unchecked)

        # Update the in-memory values and durably persist them (fsync'd). The
        # view must never decrease — it's the fencing token. op/commit_op MAY
        # decrease: they track how much of the log this node actually holds, and
        # a full_sync to a less-complete leader legitimately truncates an
        # uncommitted tail. On a write/fsync error, exit 1 (see class doc).
        def save(view : UInt64 = @view, op : UInt64 = @op, commit_op : UInt64 = @commit_op) : Nil
          @lock.synchronize do
            raise Error.new("view must not decrease (#{@view} -> #{view})") if view < @view
            @view = view
            @op = op
            @commit_op = commit_op
            persist!(fsync: true)
          end
        end

        # Persist a new op high-water WITHOUT its own fsync, for the per-ack hot
        # path: the caller (the follower's send_ack_loop) issues a syncfs over the
        # whole data dir right after, which flushes this file too. The op is only
        # advanced here AFTER the data it covers is on disk, so a crash before the
        # syncfs just leaves a stale-low op (safe — never stale-high). The atomic
        # temp+rename means a torn write can't corrupt the live file.
        def save_op_pending(op : UInt64, commit_op : UInt64 = @commit_op) : Nil
          @lock.synchronize do
            @op = op
            @commit_op = commit_op
            persist!(fsync: false)
          end
        end

        # Update the in-memory commit point (monotonic) WITHOUT persisting. The
        # VR::Node calls this as it learns the cluster commit_op from heartbeats /
        # view changes; the value is flushed to disk opportunistically by the next
        # save_op_pending (the follower's per-ack syncfs). It's read by the data
        # client to refuse a full_sync from a leader that is behind on committed
        # data (which would truncate committed records). Best-effort: if it hasn't
        # been persisted before a crash it's only stale-low, which is safe (the
        # guard may miss, never false-trip). The node and client share one State;
        # @lock makes the concurrent access safe.
        def note_commit(commit_op : UInt64) : Nil
          @lock.synchronize do
            @commit_op = commit_op if commit_op > @commit_op
          end
        end

        private def persist!(fsync : Bool) : Nil
          write_durable(fsync)
        rescue ex : IO::Error
          Log.fatal(exception: ex) { "Failed to persist #{FILENAME}: #{ex.message}" }
          exit 1
        end

        # Atomically replace the file (temp + rename). When `fsync` is true also
        # fsync the temp file and the directory so it survives a crash on its own;
        # when false the caller's syncfs provides durability.
        private def write_durable(fsync : Bool) : Nil
          Dir.mkdir_p(@data_dir)
          tmp = File.join(@data_dir, "#{FILENAME}.tmp")
          File.open(tmp, "w") do |f|
            f.write MAGIC
            f.write_bytes VERSION
            f.write_bytes @view, IO::ByteFormat::LittleEndian
            f.write_bytes @op, IO::ByteFormat::LittleEndian
            f.write_bytes @commit_op, IO::ByteFormat::LittleEndian
            f.flush
            f.fsync if fsync
          end
          File.rename(tmp, File.join(@data_dir, FILENAME))
          fsync_dir if fsync
        end

        private def fsync_dir : Nil
          fd = LibC.open(@data_dir.check_no_null_byte, LibC::O_RDONLY)
          raise IO::Error.from_errno("Failed to open #{@data_dir}") if fd < 0
          begin
            raise IO::Error.from_errno("fsync") if LibC.fsync(fd) != 0
          ensure
            LibC.close(fd)
          end
        end
      end
    end
  end
end
