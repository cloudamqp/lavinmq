require "file_utils"
require "./ops"
require "./file_table"
require "../../logger"
require "../../schema"

module LavinMQ::Clustering::Raft
  # Applies the op batches of the Raft log to the data dir, and undoes them
  # when the log is truncated.
  #
  # Appends are written as entries are appended, before they're committed: a
  # truncation cuts the files back. What can't be cut back goes to the
  # trash, `.raft_trash/<tid>/<path>`: a deleted file, a directory, what a
  # replace took the place of, what a file was truncated by. A truncation
  # moves it back, compaction purges it, so it's only gone once committed and
  # on disk here, and every responsive peer has it.
  #
  # On the leader the broker changes its files itself (the replicator moves
  # what it deletes to the trash), so its own entries only update the tables,
  # see #apply. An undo works on the files on every node: a deposed leader's
  # uncommitted changes are undone the same way.
  #
  # `base` is the table as of the compacted index, `current` as of the last
  # entry applied.
  class FileApplier
    Log            = LavinMQ::Log.for "clustering.raft.files"
    TRASH_DIR      = ".raft_trash"
    QUARANTINE_DIR = ".raft_quarantine"

    getter data_dir : String
    getter base : FileTable
    getter current : FileTable
    getter base_index : Int64
    # Files are missing or short, only a snapshot can fix that
    property? needs_snapshot = false

    # Trash the log refers to, with the index of the entry that put it there
    @trash = Deque(Tuple(Int64, Int64)).new
    # Files gone from `current` that are in the trash: their trash path
    # (relative) and the index that put them there, for sending payload
    @retired = Hash(UInt32, Tuple(String, Int64)).new
    @files = Hash(UInt32, File).new
    @touched_files = Set(UInt32).new
    @touched_dirs = Set(String).new

    def initialize(@data_dir : String, @base_index : Int64)
      @base = load_base
      @current = @base.dup
    end

    # Before the data was in the Raft log there were no tables, the log only
    # held the ISR and the membership
    private def load_base : FileTable
      FileTable.load(@data_dir, @base_index)
    rescue ex : FileTable::CorruptError
      raise ex if Dir.children(@data_dir).any? { |n| FileTable.file?(n) }
      FileTable.new
    end

    # The tables only, for entries already on disk, as when starting
    def replay(index : Int64, data : Bytes?) : Nil
      return unless data
      Ops.each(data) { |op| apply_to_table(@current, index, op, track: true) }
    end

    # Applies the batch of the entry at *index*: to the files, unless *local*
    # (this node's broker made the changes), and to the tables. *payload* has
    # the bytes of the Append ops.
    def apply(index : Int64, data : Bytes?, payload : Bytes? = nil, local = false) : Nil
      return unless data
      offset = 0i64
      Ops.each(data) do |op|
        unless local
          offset = apply_to_files(op, payload, offset)
        end
        apply_to_table(@current, index, op, track: true)
      end
    end

    # Undoes the entries from *from* on, before the log is truncated there.
    # The log must still have them.
    def undo(from : Int64, log : LogStore) : Nil
      table = @base.dup
      (@base_index + 1...from).each { |i| replay_into(table, i, log.entry(i).data) }
      before = table.dup
      undos = Array(Proc(Nil)).new
      (from..log.last_index).each do |i|
        data = log.entry(i).data || next
        Ops.each(data) do |op|
          undos << undo_of(table, op)
          apply_to_table(table, i, op, track: false)
        end
      end
      close_files
      undos.reverse_each &.call
      @current = before
      @trash.reject! { |t| t[0] >= from }
      @retired.reject! { |_, r| r[1] >= from }
    end

    # Starts over at *index*, as when a snapshot is installed. The files are
    # as the snapshot brings them, see the snapshot client.
    def reset(index : Int64, table : FileTable) : Nil
      close_files
      @base = table
      @current = table.dup
      @base_index = index
      @trash.clear
      @retired.clear
    end

    # Where the payload of *entries* is on this node, the leader: a range per
    # Append op, nil when they have none. Raises Unavailable when a file is
    # gone.
    def payload_plan(entries : Array(Entry)) : Array(PayloadRange)?
      plan = nil
      entries.each do |entry|
        data = entry.data || next
        Ops.each(data) do |op|
          next unless op.type.append?
          path, ino = locate(op.id)
          (plan ||= Array(PayloadRange).new) << PayloadRange.new(path, op.pos, op.len, ino)
        end
      end
      plan
    end

    class Unavailable < Exception; end

    # The absolute path and inode of a file, in the trash too
    private def locate(id : UInt32) : Tuple(String, UInt64)
      if e = @current[id]?
        e.ino = inode(abs(e.path)) if e.ino.zero?
        {abs(e.path), e.ino}
      elsif r = @retired[id]?
        path = abs(r[0])
        {path, inode(path)}
      else
        raise Unavailable.new("File #{id} is gone")
      end
    end

    private def inode(path : String) : UInt64
      FileApplier.inode(path) || raise Unavailable.new("#{path} is gone")
    end

    # The inode of *path*, nil if there's no such file
    def self.inode(path : String) : UInt64?
      return unless LibC.stat(path.check_no_null_byte, out stat) == 0
      stat.st_ino.to_u64
    end

    # The inode of an open file
    def self.inode(file : File) : UInt64
      raise IO::Error.from_errno("fstat") unless LibC.fstat(file.fd, out stat) == 0
      stat.st_ino.to_u64
    end

    # Fsyncs what was written since the last call, files and directories
    def sync : Nil
      @touched_files.each do |id|
        @files[id]?.try &.fsync
      end
      @touched_files.clear
      @touched_dirs.each do |dir|
        File.open(dir, &.fsync) if Dir.exists?(dir)
      end
      @touched_dirs.clear
    end

    # Folds the entries up to *index* into `base`, saves it, and purges the
    # trash they put there. The table files of other indexes are deleted by
    # #drop_old_tables, once the raft state names *index*.
    def compact(index : Int64, log : LogStore) : Nil
      return if index <= @base_index
      (@base_index + 1..index).each { |i| replay_into(@base, i, log.entry(i).data) }
      while (t = @trash.first?) && t[0] <= index
        @trash.shift
        FileUtils.rm_rf(trash_root(t[1]))
        @base.trash_watermark = Math.max(@base.trash_watermark, t[1])
      end
      @retired.reject! { |_, r| r[1] <= index }
      @current.trash_watermark = @base.trash_watermark
      @base.save(@data_dir, index)
      @base_index = index
    end

    # Deletes the table files of other indexes than the base's
    def drop_old_tables : Nil
      Dir.each_child(@data_dir) do |name|
        next unless FileTable.file?(name)
        next if name == "#{FileTable::PREFIX}#{@base_index}"
        File.delete?(File.join(@data_dir, name))
      end
    end

    # Makes the data dir match `current`, after #replay of what the journal
    # has, as when starting or after the broker stopped serving:
    # - trash the log doesn't refer to is purged if it's at or below the
    #   watermark (a purge that was cut short), else moved back (moved by a
    #   broker whose changes never made it to the log)
    # - unknown staged files are deleted
    # - files longer than the table are cut back, ones missing or short need
    #   a snapshot
    # - unknown files in the broker's directories are moved to quarantine
    def reconcile : Nil
      close_files
      reconcile_trash
      reconcile_staging
      reconcile_sizes
      quarantine_unknown unless @current.empty?
    end

    def close : Nil
      close_files
    end

    private def reconcile_trash : Nil
      return unless Dir.exists?(abs(TRASH_DIR))
      referenced = @trash.map(&.[1]).to_set
      Dir.children(abs(TRASH_DIR)).each do |name|
        tid = name.to_i64? || next
        next if referenced.includes?(tid)
        if tid <= @base.trash_watermark
          FileUtils.rm_rf(trash_root(tid))
        else
          Log.info { "Restoring #{trash_root(tid)}, its change isn't in the log" }
          restore_trash(tid)
        end
      end
    end

    private def reconcile_staging : Nil
      return unless Dir.exists?(abs(FileTable::STAGING_DIR))
      Dir.children(abs(FileTable::STAGING_DIR)).each do |name|
        id = name.to_u32?
        next if id && @current[id]?.try(&.staging?)
        File.delete?(File.join(abs(FileTable::STAGING_DIR), name))
      end
    end

    private def reconcile_sizes : Nil
      @current.each do |e|
        next if e.dir?
        info = File.info?(abs(e.path))
        if info.nil?
          Log.warn { "#{e.path} is missing, a snapshot is needed" }
          @needs_snapshot = true
        elsif info.size > e.size
          Log.info { "Cutting #{e.path} back from #{info.size} to #{e.size} bytes" }
          File.open(abs(e.path), "r+") { |f| f.truncate(e.size); f.fsync }
        elsif info.size < e.size
          Log.warn { "#{e.path} has #{info.size} bytes, #{e.size} expected, a snapshot is needed" }
          @needs_snapshot = true
        end
      end
    end

    private def abs(path : String) : String
      File.join(@data_dir, path)
    end

    private def trash_root(tid : Int64) : String
      File.join(@data_dir, TRASH_DIR, tid.to_s)
    end

    private def trash_path(tid : Int64, path : String) : String
      File.join(trash_root(tid), path)
    end

    private def replay_into(table : FileTable, index : Int64, data : Bytes?) : Nil
      return unless data
      Ops.each(data) { |op| apply_to_table(table, index, op, track: false) }
    end

    # The op's effect on *table*. With *track* the trash it refers to is
    # remembered for compaction.
    private def apply_to_table(table : FileTable, index : Int64, op : Ops::Op, track : Bool) : Nil
      case op.type
      in .create?
        path = op.flags & Ops::STAGING != 0 ? FileTable.staging_path(op.id) : op.path!
        table.add FileTable::FileEntry.new(op.id, path, 0i64, op.flags)
      in .append?, .append_inline?
        e = table[op.id]
        e.size = Math.max(e.size, op.pos + op.len)
      in .install?
        e = table[op.id]
        target = op.path!
        table.by_path?(target).try do |old|
          table.remove(old.id)
          @retired[old.id] = {File.join(TRASH_DIR, op.tid.to_s, target), index} if track
        end
        table.move(e, target)
        e.flags &= ~Ops::STAGING
        @trash << {index, op.tid} if track
      in .delete?
        table.remove(op.id).try do |e|
          @retired[op.id] = {File.join(TRASH_DIR, op.tid.to_s, e.path), index} if track
        end
        @trash << {index, op.tid} if track
      in .delete_dir?
        table.under(op.path!).each do |e|
          table.remove(e.id)
          @retired[e.id] = {File.join(TRASH_DIR, op.tid.to_s, e.path), index} if track
        end
        @trash << {index, op.tid} if track
      in .truncate_to?
        table[op.id].size = op.pos
        @trash << {index, op.tid} if track
      in .adopt?
        table.add FileTable::FileEntry.new(op.id, op.path!, op.pos)
      end
    end

    # Changes the files for an op, returns how far into the payload it has
    # read
    private def apply_to_files(op : Ops::Op, payload : Bytes?, offset : Int64) : Int64
      case op.type
      in .create?
        if op.flags & Ops::DIR != 0
          Dir.mkdir_p(abs(op.path!))
        else
          path = abs(op.flags & Ops::STAGING != 0 ? FileTable.staging_path(op.id) : op.path!)
          Dir.mkdir_p(File.dirname(path))
          File.open(path, "w") { }
          @touched_dirs << File.dirname(path)
        end
      in .append?
        bytes = payload
        if bytes.nil? || offset + op.len > bytes.size
          # As from a voter catching up a candidate, which has no payload
          Log.warn { "No payload for #{op.len} bytes to #{@current[op.id]?.try(&.path)}, a snapshot is needed" }
          @needs_snapshot = true
          return offset
        end
        write(op.id, op.pos, bytes[offset, op.len])
        return offset + op.len
      in .append_inline?
        write(op.id, op.pos, op.bytes!)
      in .install?
        e = @current[op.id]
        target = op.path!
        close_file(op.id)
        if old = @current.by_path?(target)
          close_file(old.id)
          move(abs(target), trash_path(op.tid, target))
        end
        move(abs(e.path), abs(target))
      in .delete?
        if e = @current[op.id]?
          close_file(op.id)
          move(abs(e.path), trash_path(op.tid, e.path))
        end
      in .delete_dir?
        path = op.path!
        @current.under(path).each { |e| close_file(e.id) }
        move(abs(path), trash_path(op.tid, path)) if File.exists?(abs(path))
      in .truncate_to?
        e = @current[op.id]
        close_file(op.id)
        save_tail(abs(e.path), op.pos, trash_path(op.tid, e.path))
      in .adopt?
        @needs_snapshot = true
      end
      offset
    end

    # What undoes *op*, given the table before it
    private def undo_of(table : FileTable, op : Ops::Op) : Proc(Nil)
      case op.type
      in .create?
        path = abs(op.flags & Ops::STAGING != 0 ? FileTable.staging_path(op.id) : op.path!)
        dir = op.flags & Ops::DIR != 0
        -> { dir ? (Dir.delete(path) rescue nil) : File.delete?(path); nil }
      in .append?, .append_inline?
        path = abs(table[op.id].path)
        pos = op.pos
        -> { cut(path, pos) }
      in .install?
        e = table[op.id]
        target = op.path!
        staged = abs(e.path)
        had_old = !table.by_path?(target).nil?
        trashed = trash_path(op.tid, target)
        -> {
          move(abs(target), staged) if File.exists?(abs(target))
          move(trashed, abs(target)) if had_old && File.exists?(trashed)
          nil
        }
      in .delete?
        path = table[op.id].path
        trashed = trash_path(op.tid, path)
        -> { move(trashed, abs(path)) if File.exists?(trashed); nil }
      in .delete_dir?
        path = op.path!
        trashed = trash_path(op.tid, path)
        -> { move(trashed, abs(path)) if File.exists?(trashed); nil }
      in .truncate_to?
        path = abs(table[op.id].path)
        tail = trash_path(op.tid, table[op.id].path)
        -> { restore_tail(path, tail); nil }
      in .adopt?
        -> { }
      end
    end

    private def write(id : UInt32, pos : Int64, bytes : Bytes) : Nil
      file = @files[id]? || begin
        close_files if @files.size >= 256
        @files[id] = File.open(abs(@current[id].path), "r+").tap(&.sync = true)
      end
      file.pos = pos
      file.write bytes
      @touched_files << id
    rescue File::NotFoundError
      Log.warn { "Can't write to #{@current[id]?.try(&.path)}, it's missing, a snapshot is needed" }
      @needs_snapshot = true
    end

    private def move(from : String, to : String) : Nil
      Dir.mkdir_p(File.dirname(to))
      File.rename(from, to)
      @touched_dirs << File.dirname(from) << File.dirname(to)
    end

    private def cut(path : String, size : Int64) : Nil
      return unless File.exists?(path)
      File.open(path, "r+") do |f|
        f.truncate(size) if f.size > size
      end
      @touched_dirs << File.dirname(path)
    end

    # Moves the bytes of *path* from *size* on to *tail*, then cuts it
    private def save_tail(path : String, size : Int64, tail : String) : Nil
      Dir.mkdir_p(File.dirname(tail))
      File.open(path, "r+") do |f|
        f.seek(size)
        File.open(tail, "w") do |t|
          IO.copy(f, t)
          t.fsync
        end
        f.truncate(size)
      end
      @touched_dirs << File.dirname(tail)
    end

    private def restore_tail(path : String, tail : String) : Nil
      return unless File.exists?(tail)
      File.open(path, "a") do |f|
        File.open(tail) { |t| IO.copy(t, f) }
      end
      File.delete(tail)
    end

    # Moves what's in a trash entry back where it was
    private def restore_trash(tid : Int64) : Nil
      root = trash_root(tid)
      each_file(root) do |rel|
        target = abs(rel)
        if File.exists?(target)
          move(target, File.join(abs(QUARANTINE_DIR), Time.utc.to_unix_ms.to_s, rel))
        end
        move(File.join(root, rel), target)
      end
      FileUtils.rm_rf(root)
    end

    # Files in subdirectories of the data dir that the table doesn't know of,
    # e.g. a segment a fenced broker created, moved out of the broker's way
    private def quarantine_unknown : Nil
      Dir.each_child(@data_dir) do |name|
        next if Schema.node_local_file?(name)
        next unless Dir.exists?(abs(name))
        each_file(abs(name)) do |rel|
          path = File.join(name, rel)
          next if @current.by_path?(path)
          Log.warn { "#{path} isn't in the raft log, moving it to #{QUARANTINE_DIR}" }
          move(abs(path), File.join(abs(QUARANTINE_DIR), path))
        end
      end
    end

    # Yields the regular files under *dir*, relative to it
    private def each_file(dir : String, prefix = "", &block : String ->) : Nil
      return unless Dir.exists?(dir)
      Dir.each_child(dir) do |name|
        path = File.join(dir, name)
        rel = prefix.empty? ? name : File.join(prefix, name)
        if File.directory?(path)
          each_file(path, rel, &block)
        elsif File.file?(path)
          yield rel
        end
      end
    end

    private def close_file(id : UInt32) : Nil
      if f = @files.delete(id)
        f.fsync if @touched_files.delete(id)
        f.close
      end
    end

    private def close_files : Nil
      @files.each do |id, f|
        f.fsync if @touched_files.includes?(id)
        f.close
      end
      @files.clear
      @touched_files.clear
    end
  end
end
