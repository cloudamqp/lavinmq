require "digest/crc32"
require "../mfile"
require "../filesystem"
require "./protocol"

module LavinMQ
  module ClusteringV2
    class RaftLog
      IDX_MAGIC        = 0x32584449_u32 # "IDX2"
      META_MAGIC       = 0x3254454D_u32 # "MET2"
      IDX_HEADER_SIZE  =         64_i64
      DEFAULT_CAPACITY =     65_536_u64
      CTRL_INIT_SIZE   = 8 * 1024 * 1024 # 8 MiB control log segment

      alias SegmentKey = Tuple(UInt16, UInt32, UInt32)
      alias TruncateCallback = LogIndexEntry -> Nil

      getter data_dir : String
      getter capacity : UInt64
      getter cluster_id : UInt64 = 0_u64
      getter node_id : UInt64 = 0_u64
      getter current_term : UInt64 = 0_u64
      getter voted_for : UInt64 = 0_u64
      getter last_snapshot_lsn : UInt64 = 0_u64
      getter last_snapshot_term : UInt64 = 0_u64
      getter first_lsn : UInt64 = 1_u64
      getter last_lsn : UInt64 = 0_u64
      getter commit_index : UInt64 = 0_u64
      getter members = Array(MemberInfo).new

      @mask : UInt64
      @idx_mfile : MFile
      @ctrl_mfile : MFile
      @lock = Mutex.new(:reentrant)
      @segments = Hash(SegmentKey, MFile).new
      @deleted_segments = Hash(SegmentKey, UInt64).new
      @on_truncate : TruncateCallback?
      @closed = false

      def initialize(@data_dir : String,
                     capacity : UInt64 = DEFAULT_CAPACITY,
                     node_id : UInt64 = 0_u64,
                     cluster_id : UInt64 = 0_u64)
        raise ArgumentError.new("capacity must be a power of 2") unless capacity > 0 && (capacity & (capacity - 1)) == 0
        @capacity = capacity
        @mask = capacity - 1
        @node_id = node_id
        @cluster_id = cluster_id

        FileUtils.mkdir_p(@data_dir)
        load_or_init_meta
        @idx_mfile = open_or_init_idx
        @ctrl_mfile = open_or_init_ctrl
      end

      def on_truncate(&block : LogIndexEntry -> Nil) : Nil
        @on_truncate = block
      end

      def set_term_and_vote(term : UInt64, voted_for : UInt64 = 0_u64) : Nil
        @lock.synchronize do
          @current_term = term
          @voted_for = voted_for
          save_meta!
        end
      end

      # ameba:disable Naming/AccessorMethodName
      def set_members(members : Array(MemberInfo)) : Nil
        @lock.synchronize do
          @members = members.dup
          save_meta!
        end
      end

      def upsert_member(member : MemberInfo) : Nil
        @lock.synchronize do
          if idx = @members.index { |m| m.node_id == member.node_id }
            @members[idx] = member
          else
            @members << member
          end
          save_meta!
        end
      end

      def remove_member(node_id : UInt64) : Nil
        @lock.synchronize do
          @members.reject! { |m| m.node_id == node_id }
          save_meta!
        end
      end

      def register_segment(vhost_id : UInt16, queue_id : UInt32, segment_id : UInt32, mfile : MFile) : Nil
        @lock.synchronize do
          key = {vhost_id, queue_id, segment_id}
          @segments[key] = mfile
          @deleted_segments.delete(key)
        end
      end

      def unregister_segment(vhost_id : UInt16, queue_id : UInt32, segment_id : UInt32) : Nil
        @lock.synchronize do
          @segments.delete({vhost_id, queue_id, segment_id})
        end
      end

      def last_log_term : UInt64
        @lock.synchronize do
          return @last_snapshot_term if @last_lsn == @last_snapshot_lsn || @last_lsn == 0
          term_at_unsafe(@last_lsn)
        end
      end

      def term_at(lsn : UInt64) : UInt64?
        @lock.synchronize do
          return 0_u64 if lsn == 0
          return @last_snapshot_term if lsn == @last_snapshot_lsn
          return if lsn < @first_lsn || lsn > @last_lsn
          term_at_unsafe(lsn)
        end
      end

      private def term_at_unsafe(lsn : UInt64) : UInt64
        entry_ptr(lsn).value.term
      end

      # Appends a zero-copy Publish entry referencing an existing queue MFile segment.
      def append_publish(term : UInt64,
                         vhost_id : UInt16,
                         queue_id : UInt32,
                         segment_id : UInt32,
                         position : UInt32,
                         byte_size : UInt32,
                         fallback_payload : Bytes? = nil) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          flags = 0_u8
          pos = position
          len = byte_size
          key = {vhost_id, queue_id, segment_id}
          if !@segments.has_key?(key) && (payload = fallback_payload)
            flags |= LogIndexEntry::FLAG_HAS_CTRL
            pos = append_staged_publish_ctrl(position, payload)
            len = (payload.size + 4).to_u32
          end
          entry = LogIndexEntry.new(
            term: term,
            lsn: lsn,
            vhost_id: vhost_id,
            cmd_type: LogCommandType::Publish.value,
            flags: flags,
            queue_id: queue_id,
            segment_id: segment_id,
            position: pos,
            byte_size: len
          )
          write_index_entry_unsafe(entry)
          entry
        end
      end

      # Rebinds a follower's staged Publish entry directly to its local MFile segment
      # once the Hot-Standby Applier has written the message at `position`.
      def bind_publish_segment(lsn : UInt64, segment_id : UInt32, position : UInt32, byte_size : UInt32) : Nil
        @lock.synchronize do
          return if lsn < @first_lsn || lsn > @last_lsn
          ptr = entry_ptr(lsn)
          cur = ptr.value
          return unless cur.lsn == lsn && cur.command.publish?
          ptr.value = LogIndexEntry.new(
            term: cur.term,
            lsn: cur.lsn,
            vhost_id: cur.vhost_id,
            cmd_type: cur.cmd_type,
            flags: cur.flags & ~LogIndexEntry::FLAG_HAS_CTRL,
            queue_id: cur.queue_id,
            segment_id: segment_id,
            position: position,
            byte_size: byte_size
          )
        end
      end

      # Appends a compact 36-byte Ack entry (no payload needed beyond LogIndexEntry fields).
      def append_ack(term : UInt64,
                     vhost_id : UInt16,
                     queue_id : UInt32,
                     segment_id : UInt32,
                     position : UInt32) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          entry = LogIndexEntry.new(
            term: term,
            lsn: lsn,
            vhost_id: vhost_id,
            cmd_type: LogCommandType::Ack.value,
            flags: 0_u8,
            queue_id: queue_id,
            segment_id: segment_id,
            position: position,
            byte_size: 0_u32
          )
          write_index_entry_unsafe(entry)
          entry
        end
      end

      # Appends a SegmentDelete entry and records the deletion LSN so older Publish/Ack
      # entries for this segment can be elided during follower catch-up.
      def append_segment_delete(term : UInt64,
                                vhost_id : UInt16,
                                queue_id : UInt32,
                                segment_id : UInt32) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          key = {vhost_id, queue_id, segment_id}
          @segments.delete(key)
          @deleted_segments[key] = lsn
          entry = LogIndexEntry.new(
            term: term,
            lsn: lsn,
            vhost_id: vhost_id,
            cmd_type: LogCommandType::SegmentDelete.value,
            flags: 0_u8,
            queue_id: queue_id,
            segment_id: segment_id,
            position: 0_u32,
            byte_size: 0_u32
          )
          write_index_entry_unsafe(entry)
          entry
        end
      end

      # Appends a general command (e.g., LeaderNoOp, ConfChange, QueueDeclare, StreamOffsetCommit).
      def append_command(term : UInt64,
                         cmd_type : LogCommandType,
                         payload : Bytes = Bytes.empty,
                         vhost_id : UInt16 = 0_u16,
                         queue_id : UInt32 = 0_u32,
                         segment_id : UInt32 = 0_u32,
                         position : UInt32 = 0_u32) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          flags = 0_u8
          pos = position
          len = payload.size.to_u32
          unless payload.empty?
            flags |= LogIndexEntry::FLAG_HAS_CTRL
            pos = append_ctrl_bytes(payload)
          end
          entry = LogIndexEntry.new(
            term: term,
            lsn: lsn,
            vhost_id: vhost_id,
            cmd_type: cmd_type.value,
            flags: flags,
            queue_id: queue_id,
            segment_id: segment_id,
            position: pos,
            byte_size: len
          )
          write_index_entry_unsafe(entry)
          entry
        end
      end

      # Appends a pre-constructed LogEntryRecord received from a leader in AppendEntries.
      def append_follower_record(record : LogEntryRecord) : LogIndexEntry
        @lock.synchronize do
          in_entry = record.entry
          payload = record.payload
          flags = in_entry.flags
          pos = in_entry.position
          len = in_entry.byte_size

          if in_entry.command.segment_delete?
            key = {in_entry.vhost_id, in_entry.queue_id, in_entry.segment_id}
            @segments.delete(key)
            @deleted_segments[key] = in_entry.lsn
          end

          if !payload.empty?
            flags |= LogIndexEntry::FLAG_HAS_CTRL
            if in_entry.command.publish?
              pos = append_staged_publish_ctrl(in_entry.position, payload)
              len = (payload.size + 4).to_u32
            else
              pos = append_ctrl_bytes(payload)
              len = payload.size.to_u32
            end
          end

          stored = LogIndexEntry.new(
            term: in_entry.term,
            lsn: in_entry.lsn,
            vhost_id: in_entry.vhost_id,
            cmd_type: in_entry.cmd_type,
            flags: flags,
            queue_id: in_entry.queue_id,
            segment_id: in_entry.segment_id,
            position: pos,
            byte_size: len
          )
          write_index_entry_unsafe(stored)
          stored
        end
      end

      # Standard Raft AppendEntries log consistency check + conflict truncation + append.
      def maybe_append(prev_log_lsn : UInt64,
                       prev_log_term : UInt64,
                       leader_commit : UInt64,
                       entries : Array(LogEntryRecord)) : Tuple(Bool, UInt64, UInt64, UInt64)
        @lock.synchronize do
          if prev_log_lsn > @last_lsn
            return {false, @last_lsn, @last_lsn + 1, 0_u64}
          end

          if prev_log_lsn > 0
            local_prev_term = term_at(prev_log_lsn)
            if local_prev_term.nil?
              return {false, @last_lsn, @first_lsn, 0_u64}
            elsif local_prev_term != prev_log_term
              conflict_term = local_prev_term
              conflict_lsn = prev_log_lsn
              while conflict_lsn > @first_lsn && term_at_unsafe(conflict_lsn - 1) == conflict_term
                conflict_lsn -= 1
              end
              return {false, @last_lsn, conflict_lsn, conflict_term}
            end
          end

          entries.each do |rec|
            existing_term = term_at(rec.lsn)
            if existing_term && existing_term != rec.term
              truncate_from_unsafe(rec.lsn)
              append_follower_record(rec)
            elsif existing_term.nil?
              append_follower_record(rec)
            end
          end

          if leader_commit > @commit_index
            new_commit = Math.min(leader_commit, @last_lsn)
            advance_commit_index_unsafe(new_commit) if new_commit > @commit_index
          end

          {true, @last_lsn, 0_u64, 0_u64}
        end
      end

      def advance_commit_index(new_commit : UInt64) : Bool
        @lock.synchronize do
          return false if new_commit <= @commit_index || new_commit > @last_lsn
          advance_commit_index_unsafe(new_commit)
          true
        end
      end

      private def advance_commit_index_unsafe(new_commit : UInt64) : Nil
        @commit_index = new_commit
        write_idx_header_unsafe
      end

      # Truncates uncommitted entries from `from_lsn` through `@last_lsn` inclusive.
      # Never truncates committed entries (`from_lsn <= @commit_index` raises).
      def truncate_from(from_lsn : UInt64) : Nil
        @lock.synchronize do
          truncate_from_unsafe(from_lsn)
        end
      end

      private def truncate_from_unsafe(from_lsn : UInt64) : Nil
        return if from_lsn > @last_lsn
        if from_lsn <= @commit_index
          raise ArgumentError.new("Cannot truncate committed entry lsn=#{from_lsn} (commit_index=#{@commit_index})")
        end

        lsn = @last_lsn
        while lsn >= from_lsn
          entry = normalize_entry_unsafe(entry_ptr(lsn).value)
          @on_truncate.try &.call(entry)
          entry_ptr(lsn).value = LogIndexEntry.new
          if lsn == 1
            break
          end
          lsn -= 1
        end

        @last_lsn = from_lsn - 1
        if @last_lsn < @first_lsn
          @first_lsn = @last_lsn + 1
        end
        write_idx_header_unsafe
      end

      # Reads a single log entry at `lsn`.
      # If the entry is a Publish or Ack for a segment that has already been deleted,
      # returns a Tombstone entry so followers can skip obsolete segment bytes without a snapshot.
      def read_entry(lsn : UInt64) : LogEntryRecord?
        @lock.synchronize do
          return if lsn < @first_lsn || lsn > @last_lsn
          raw_entry = entry_ptr(lsn).value
          return if raw_entry.lsn != lsn
          entry = normalize_entry_unsafe(raw_entry)

          if entry.command.publish? || entry.command.ack?
            key = {entry.vhost_id, entry.queue_id, entry.segment_id}
            if (del_lsn = @deleted_segments[key]?) && del_lsn > lsn
              tombstone = LogIndexEntry.new(
                term: entry.term,
                lsn: entry.lsn,
                vhost_id: entry.vhost_id,
                cmd_type: LogCommandType::Tombstone.value,
                flags: LogIndexEntry::FLAG_TOMBSTONED,
                queue_id: entry.queue_id,
                segment_id: entry.segment_id,
                position: entry.position,
                byte_size: 0_u32
              )
              return LogEntryRecord.new(tombstone, Bytes.empty)
            end
          end

          payload = resolve_payload_unsafe(raw_entry)
          LogEntryRecord.new(entry, payload)
        end
      end

      def read_entries(from_lsn : UInt64, max_count : Int32 = 1024, max_bytes : Int32 = 4 * 1024 * 1024) : Array(LogEntryRecord)?
        @lock.synchronize do
          return Array(LogEntryRecord).new if from_lsn > @last_lsn
          return if from_lsn < @first_lsn

          result = Array(LogEntryRecord).new
          bytes_accum = 0
          lsn = from_lsn
          while lsn <= @last_lsn && result.size < max_count
            rec = read_entry(lsn) || return
            result << rec
            bytes_accum += sizeof(LogIndexEntry) + rec.payload.size
            lsn += 1
            break if bytes_accum >= max_bytes
          end
          result
        end
      end

      # Resets the log to a snapshot boundary (`last_included_lsn`, `last_included_term`).
      def install_snapshot_state(last_included_lsn : UInt64,
                                 last_included_term : UInt64,
                                 members : Array(MemberInfo)) : Nil
        @lock.synchronize do
          @last_snapshot_lsn = last_included_lsn
          @last_snapshot_term = last_included_term
          @members = members.dup
          if @last_lsn <= last_included_lsn
            @first_lsn = last_included_lsn + 1
            @last_lsn = last_included_lsn
          elsif @first_lsn <= last_included_lsn
            @first_lsn = last_included_lsn + 1
          end
          @commit_index = Math.max(@commit_index, last_included_lsn)
          write_idx_header_unsafe
          save_meta!
        end
      end

      # Checks whether candidate's log is at least as up-to-date as ours (Raft §5.4.1).
      def up_to_date?(candidate_last_lsn : UInt64, candidate_last_term : UInt64) : Bool
        @lock.synchronize do
          my_term = last_log_term
          if candidate_last_term != my_term
            candidate_last_term > my_term
          else
            candidate_last_lsn >= @last_lsn
          end
        end
      end

      def fsync : Nil
        @lock.synchronize do
          @idx_mfile.fsync
          @ctrl_mfile.fsync
        end
      end

      def close : Nil
        @lock.synchronize do
          return if @closed
          @closed = true
          @idx_mfile.close(truncate_to_size: false)
          @ctrl_mfile.close
        end
      end

      private def normalize_entry_unsafe(raw_entry : LogIndexEntry) : LogIndexEntry
        if raw_entry.command.publish? && raw_entry.has_ctrl_payload? && raw_entry.byte_size >= 4
          hdr = @ctrl_mfile.to_slice(raw_entry.position.to_i64, 4)
          seg_pos = IO::ByteFormat::LittleEndian.decode(UInt32, hdr)
          return LogIndexEntry.new(
            term: raw_entry.term,
            lsn: raw_entry.lsn,
            vhost_id: raw_entry.vhost_id,
            cmd_type: raw_entry.cmd_type,
            flags: raw_entry.flags & ~LogIndexEntry::FLAG_HAS_CTRL,
            queue_id: raw_entry.queue_id,
            segment_id: raw_entry.segment_id,
            position: seg_pos,
            byte_size: raw_entry.byte_size - 4_u32
          )
        end
        raw_entry
      end

      private def resolve_payload_unsafe(entry : LogIndexEntry) : Bytes
        return Bytes.empty if entry.byte_size == 0

        if entry.has_ctrl_payload?
          if entry.command.publish?
            return Bytes.empty if entry.byte_size <= 4
            return @ctrl_mfile.to_slice((entry.position + 4).to_i64, (entry.byte_size - 4).to_i64)
          end
          return @ctrl_mfile.to_slice(entry.position.to_i64, entry.byte_size.to_i64)
        end

        if entry.command.publish?
          key = {entry.vhost_id, entry.queue_id, entry.segment_id}
          if seg_mfile = @segments[key]?
            return seg_mfile.to_slice(entry.position.to_i64, entry.byte_size.to_i64)
          elsif @deleted_segments.has_key?(key)
            return Bytes.empty
          else
            raise IO::EOFError.new("Segment #{key} not registered and not marked deleted for lsn=#{entry.lsn}")
          end
        end

        Bytes.empty
      end

      private def append_staged_publish_ctrl(seg_pos : UInt32, payload : Bytes) : UInt32
        needed = payload.size + 4
        if @ctrl_mfile.size + needed > @ctrl_mfile.capacity
          new_cap = Math.max(@ctrl_mfile.capacity * 2, @ctrl_mfile.size + needed + CTRL_INIT_SIZE)
          path = @ctrl_mfile.path
          @ctrl_mfile.close(truncate_to_size: false)
          @ctrl_mfile = MFile.new(path, new_cap)
        end
        pos = @ctrl_mfile.size.to_u32
        @ctrl_mfile.write_bytes(seg_pos, IO::ByteFormat::LittleEndian)
        @ctrl_mfile.write(payload)
        pos
      end

      private def append_ctrl_bytes(bytes : Bytes) : UInt32
        if @ctrl_mfile.size + bytes.size > @ctrl_mfile.capacity
          # Compact/reset ctrl_mfile if all entries referencing old positions have wrapped or been snapshotted,
          # or expand capacity by reopening with doubled capacity.
          new_cap = Math.max(@ctrl_mfile.capacity * 2, @ctrl_mfile.size + bytes.size + CTRL_INIT_SIZE)
          path = @ctrl_mfile.path
          @ctrl_mfile.close(truncate_to_size: false)
          @ctrl_mfile = MFile.new(path, new_cap)
        end
        pos = @ctrl_mfile.size.to_u32
        @ctrl_mfile.write(bytes)
        pos
      end

      private def write_index_entry_unsafe(entry : LogIndexEntry) : Nil
        lsn = entry.lsn
        entry_ptr(lsn).value = entry
        @last_lsn = lsn
        if @first_lsn == 0
          @first_lsn = lsn
        end
        if @last_lsn >= @first_lsn + @capacity
          @first_lsn = @last_lsn - @capacity + 1
        end
        write_idx_header_unsafe
      end

      private def entry_ptr(lsn : UInt64) : Pointer(LogIndexEntry)
        slot = (lsn - 1) & @mask
        base = @idx_mfile.to_slice.to_unsafe + IDX_HEADER_SIZE
        base.as(LogIndexEntry*) + slot
      end

      private def open_or_init_idx : MFile
        path = File.join(@data_dir, "raft.idx")
        total_bytes = IDX_HEADER_SIZE + (@capacity.to_i64 * sizeof(LogIndexEntry))
        existed = File.exists?(path) && File.size(path) >= IDX_HEADER_SIZE
        mfile = MFile.new(path, total_bytes)
        mfile.resize(total_bytes)

        if existed
          slice = mfile.to_slice
          magic = IO::ByteFormat::LittleEndian.decode(UInt32, slice[0, 4])
          stored_cap = IO::ByteFormat::LittleEndian.decode(UInt64, slice[8, 8])
          if magic == IDX_MAGIC && stored_cap == @capacity
            @first_lsn = IO::ByteFormat::LittleEndian.decode(UInt64, slice[16, 8])
            @last_lsn = IO::ByteFormat::LittleEndian.decode(UInt64, slice[24, 8])
            @commit_index = IO::ByteFormat::LittleEndian.decode(UInt64, slice[32, 8])
            return mfile
          end
        end

        @first_lsn = @last_snapshot_lsn + 1
        @last_lsn = @last_snapshot_lsn
        @commit_index = @last_snapshot_lsn
        write_idx_header_to_slice(mfile.to_slice)
        mfile
      end

      private def write_idx_header_unsafe : Nil
        write_idx_header_to_slice(@idx_mfile.to_slice)
      end

      private def write_idx_header_to_slice(slice : Bytes) : Nil
        ptr = slice.to_unsafe
        IO::ByteFormat::LittleEndian.encode(IDX_MAGIC, Slice.new(ptr, 4))
        IO::ByteFormat::LittleEndian.encode(1_u32, Slice.new(ptr + 4, 4))
        IO::ByteFormat::LittleEndian.encode(@capacity, Slice.new(ptr + 8, 8))
        IO::ByteFormat::LittleEndian.encode(@first_lsn, Slice.new(ptr + 16, 8))
        IO::ByteFormat::LittleEndian.encode(@last_lsn, Slice.new(ptr + 24, 8))
        IO::ByteFormat::LittleEndian.encode(@commit_index, Slice.new(ptr + 32, 8))
      end

      private def open_or_init_ctrl : MFile
        path = File.join(@data_dir, "raft.ctrl")
        mfile = MFile.new(path, CTRL_INIT_SIZE)
        if mfile.size == 0
          mfile.write_bytes(IDX_MAGIC, IO::ByteFormat::LittleEndian)
        end
        mfile
      end

      private def meta_path : String
        File.join(@data_dir, "raft.meta")
      end

      private def load_or_init_meta : Nil
        path = meta_path
        if File.exists?(path)
          raw = File.read(path).to_slice
          if raw.size >= 12
            magic = IO::ByteFormat::LittleEndian.decode(UInt32, raw[0, 4])
            expected_crc = IO::ByteFormat::LittleEndian.decode(UInt32, raw[4, 4])
            body = raw[8..]
            actual_crc = Digest::CRC32.checksum(body)
            if magic == META_MAGIC && expected_crc == actual_crc
              io = IO::Memory.new(body)
              fmt = IO::ByteFormat::LittleEndian
              @cluster_id = fmt.decode(UInt64, io)
              @node_id = fmt.decode(UInt64, io)
              @current_term = fmt.decode(UInt64, io)
              @voted_for = fmt.decode(UInt64, io)
              @last_snapshot_lsn = fmt.decode(UInt64, io)
              @last_snapshot_term = fmt.decode(UInt64, io)
              count = fmt.decode(UInt32, io)
              @members = Array(MemberInfo).new(count.to_i) { MemberInfo.from_io(io, fmt) }
              return
            end
          end
        end

        @cluster_id = Random::Secure.rand(UInt64) if @cluster_id == 0
        @node_id = Random::Secure.rand(1_u64..UInt64::MAX) if @node_id == 0
        save_meta!
      end

      private def save_meta! : Nil
        body_io = IO::Memory.new(256)
        fmt = IO::ByteFormat::LittleEndian
        fmt.encode(@cluster_id, body_io)
        fmt.encode(@node_id, body_io)
        fmt.encode(@current_term, body_io)
        fmt.encode(@voted_for, body_io)
        fmt.encode(@last_snapshot_lsn, body_io)
        fmt.encode(@last_snapshot_term, body_io)
        fmt.encode(@members.size.to_u32, body_io)
        @members.each(&.to_io(body_io, fmt))

        body = body_io.to_slice
        crc = Digest::CRC32.checksum(body)

        tmp_path = File.join(@data_dir, "raft.meta.tmp")
        File.open(tmp_path, "w") do |f|
          fmt.encode(META_MAGIC, f)
          fmt.encode(crc, f)
          f.write(body)
        end
        FileSystem.durable_rename(tmp_path, meta_path)
      end
    end
  end
end
