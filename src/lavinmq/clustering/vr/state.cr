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

        # Update the in-memory values (guarding monotonicity) and durably persist
        # them. On a write/fsync error, exit 1 (see class doc).
        def save(view : UInt64 = @view, op : UInt64 = @op, commit_op : UInt64 = @commit_op) : Nil
          raise Error.new("view must not decrease (#{@view} -> #{view})") if view < @view
          raise Error.new("op must not decrease (#{@op} -> #{op})") if op < @op
          raise Error.new("commit_op must not decrease (#{@commit_op} -> #{commit_op})") if commit_op < @commit_op
          @view = view
          @op = op
          @commit_op = commit_op
          persist!
        end

        private def persist! : Nil
          write_durable
        rescue ex : IO::Error
          Log.fatal(exception: ex) { "Failed to persist #{FILENAME}: #{ex.message}" }
          exit 1
        end

        # Atomic + durable: write a temp file, fsync it, rename over the real
        # file, then fsync the directory so the rename itself survives a crash.
        private def write_durable : Nil
          Dir.mkdir_p(@data_dir)
          tmp = File.join(@data_dir, "#{FILENAME}.tmp")
          File.open(tmp, "w") do |f|
            f.write MAGIC
            f.write_bytes VERSION
            f.write_bytes @view, IO::ByteFormat::LittleEndian
            f.write_bytes @op, IO::ByteFormat::LittleEndian
            f.write_bytes @commit_op, IO::ByteFormat::LittleEndian
            f.flush
            f.fsync
          end
          File.rename(tmp, File.join(@data_dir, FILENAME))
          fsync_dir
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
