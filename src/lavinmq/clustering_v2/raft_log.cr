require "digest/crc32"
require "file_utils"
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

      alias SegmentKey = Tuple(UInt64, UInt64, UInt32)
      alias TruncateCallback = LogIndexEntry -> Nil
      alias MembershipCallback = Array(MemberInfo) -> Nil

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
      getter applied_index : UInt64 = 0_u64
      getter members = Array(MemberInfo).new
      getter committed_members = Array(MemberInfo).new
      getter? meta_existed : Bool = false
      property? track_applied : Bool = false
      property? sync : Bool = true

      @snapshot_members = Array(MemberInfo).new
      @mask : UInt64
      @idx_mfile : MFile
      @ctrl_mfile : MFile
      @lock = Mutex.new(:reentrant)
      @segments = Hash(SegmentKey, MFile).new
      @segment_paths = Hash(SegmentKey, String).new
      @deleted_segments = Hash(SegmentKey, UInt64).new
      @on_truncate : TruncateCallback?
      @on_membership_change : MembershipCallback?
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
        recompute_members_unsafe if @meta_existed
      end

      def on_truncate(&block : LogIndexEntry -> Nil) : Nil
        @on_truncate = block
      end

      def on_membership_change(&block : Array(MemberInfo) -> Nil) : Nil
        @on_membership_change = block
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
        cb, new_members = @lock.synchronize do
          @snapshot_members = members.dup
          @committed_members = members.dup
          @members = members.dup
          recompute_members_unsafe
          save_meta!
          {@on_membership_change, @members.dup}
        end
        cb.try &.call(new_members)
      end

      def upsert_member(member : MemberInfo) : Nil
        cb, new_members = @lock.synchronize do
          upsert_into_list(@snapshot_members, member)
          upsert_into_list(@committed_members, member)
          upsert_into_list(@members, member)
          save_meta!
          {@on_membership_change, @members.dup}
        end
        cb.try &.call(new_members)
      end

      def update_member_uris(node_id : UInt64, peer_uri : String, amqp_uri : String, http_uri : String, mqtt_uri : String = "") : Nil
        cb, new_members = @lock.synchronize do
          update_uris_in_list(@snapshot_members, node_id, peer_uri, amqp_uri, http_uri, mqtt_uri)
          update_uris_in_list(@committed_members, node_id, peer_uri, amqp_uri, http_uri, mqtt_uri)
          update_uris_in_list(@members, node_id, peer_uri, amqp_uri, http_uri, mqtt_uri)
          save_meta!
          {@on_membership_change, @members.dup}
        end
        cb.try &.call(new_members)
      end

      def remove_member(node_id : UInt64) : Nil
        cb, new_members = @lock.synchronize do
          @snapshot_members.reject! { |m| m.node_id == node_id }
          @committed_members.reject! { |m| m.node_id == node_id }
          @members.reject! { |m| m.node_id == node_id }
          save_meta!
          {@on_membership_change, @members.dup}
        end
        cb.try &.call(new_members)
      end

      def register_segment(vhost_id : UInt64, queue_id : UInt64, segment_id : UInt32, mfile : MFile) : Nil
        @lock.synchronize do
          key = {vhost_id, queue_id, segment_id}
          mfile.lease
          if old = @segments[key]?
            old.release_lease
          end
          @segments[key] = mfile
          @segment_paths[key] = mfile.path
          @deleted_segments.delete(key)
        end
      end

      def register_segment_path(vhost_id : UInt64, queue_id : UInt64, segment_id : UInt32, path : String) : Nil
        @lock.synchronize do
          key = {vhost_id, queue_id, segment_id}
          @segment_paths[key] = path
          if old = @segments.delete(key)
            evict_segment_unsafe(key, old)
          end
        end
      end

      def unregister_segment(vhost_id : UInt64, queue_id : UInt64, segment_id : UInt32) : Nil
        @lock.synchronize do
          key = {vhost_id, queue_id, segment_id}
          if old = @segments.delete(key)
            evict_segment_unsafe(key, old)
          end
        end
      end

      def unregister_queue_segments(vhost_id : UInt64, queue_id : UInt64) : Nil
        @lock.synchronize do
          to_evict = Array(Tuple(SegmentKey, MFile)).new
          @segments.reject! do |key, mfile|
            if key[0] == vhost_id && key[1] == queue_id
              to_evict << {key, mfile}
              @deleted_segments[key] = @last_lsn + 1
              true
            else
              false
            end
          end
          to_evict.each { |(k, mf)| evict_segment_unsafe(k, mf) }
          @segment_paths.reject! { |key, _| key[0] == vhost_id && key[1] == queue_id }
        end
      end

      private def evict_segment_unsafe(key : SegmentKey, mfile : MFile) : Nil
        if !mfile.closed? && @last_lsn >= @first_lsn
          lsn = @first_lsn
          while lsn <= @last_lsn
            entry = entry_ptr(lsn).value
            if entry.lsn == lsn && entry.command.publish? && !entry.has_ctrl_payload? &&
               entry.vhost_id == key[0] && entry.queue_id == key[1] && entry.segment_id == key[2]
              end_pos = entry.position.to_u64 + entry.byte_size.to_u64
              if end_pos <= mfile.size.to_u64
                slice = mfile.to_slice(entry.position.to_i64, entry.byte_size.to_i64)
                ctrl_pos = append_staged_publish_ctrl(entry.position, slice)
                updated = LogIndexEntry.new(
                  term: entry.term,
                  lsn: entry.lsn,
                  vhost_id: entry.vhost_id,
                  cmd_type: entry.cmd_type,
                  flags: entry.flags | LogIndexEntry::FLAG_HAS_CTRL,
                  queue_id: entry.queue_id,
                  segment_id: entry.segment_id,
                  position: ctrl_pos,
                  byte_size: entry.byte_size + 4_u32
                )
                entry_ptr(lsn).value = updated
              end
            end
            lsn += 1
          end
        end
        mfile.release_lease
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
                         vhost_id : UInt64,
                         queue_id : UInt64,
                         segment_id : UInt32,
                         position : UInt32,
                         byte_size : UInt32,
                         fallback_payload : Bytes? = nil) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          flags = LogIndexEntry::FLAG_FROM_LEADER
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

      # Appends a compact 48-byte Ack entry (no payload needed beyond LogIndexEntry fields).
      def append_ack(term : UInt64,
                     vhost_id : UInt64,
                     queue_id : UInt64,
                     segment_id : UInt32,
                     position : UInt32) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          entry = LogIndexEntry.new(
            term: term,
            lsn: lsn,
            vhost_id: vhost_id,
            cmd_type: LogCommandType::Ack.value,
            flags: LogIndexEntry::FLAG_FROM_LEADER,
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
                                vhost_id : UInt64,
                                queue_id : UInt64,
                                segment_id : UInt32) : LogIndexEntry
        @lock.synchronize do
          lsn = @last_lsn + 1
          key = {vhost_id, queue_id, segment_id}
          if old = @segments.delete(key)
            old.release_lease
          end
          @segment_paths.delete(key)
          @deleted_segments[key] = lsn
          entry = LogIndexEntry.new(
            term: term,
            lsn: lsn,
            vhost_id: vhost_id,
            cmd_type: LogCommandType::SegmentDelete.value,
            flags: LogIndexEntry::FLAG_FROM_LEADER,
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
                         vhost_id : UInt64 = 0_u64,
                         queue_id : UInt64 = 0_u64,
                         segment_id : UInt32 = 0_u32,
                         position : UInt32 = 0_u32) : LogIndexEntry
        entry, cb, members_snapshot = @lock.synchronize do
          lsn = @last_lsn + 1
          flags = LogIndexEntry::FLAG_FROM_LEADER
          pos = position
          len = payload.size.to_u32
          unless payload.empty?
            flags |= LogIndexEntry::FLAG_HAS_CTRL
            pos = append_ctrl_bytes(payload)
          end
          idx_entry = LogIndexEntry.new(
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
          write_index_entry_unsafe(idx_entry)
          if cmd_type.conf_change? && !payload.empty?
            apply_conf_change_to_list(@members, payload)
            save_meta!
            {idx_entry, @on_membership_change, @members.dup}
          else
            {idx_entry, nil, nil}
          end
        end
        if cb && members_snapshot
          cb.call(members_snapshot)
        end
        entry
      end

      # Appends a pre-constructed LogEntryRecord received from a leader in AppendEntries.
      def append_follower_record(record : LogEntryRecord) : LogIndexEntry
        stored, cb, members_snapshot = @lock.synchronize do
          idx_entry, changed = append_follower_record_unsafe(record)
          if changed
            {idx_entry, @on_membership_change, @members.dup}
          else
            {idx_entry, nil, nil}
          end
        end
        if cb && members_snapshot
          cb.call(members_snapshot)
        end
        stored
      end

      private def append_follower_record_unsafe(record : LogEntryRecord) : Tuple(LogIndexEntry, Bool)
        in_entry = record.entry
        payload = record.payload
        flags = in_entry.flags & ~LogIndexEntry::FLAG_FROM_LEADER
        pos = in_entry.position
        len = in_entry.byte_size

        if in_entry.command.segment_delete?
          key = {in_entry.vhost_id, in_entry.queue_id, in_entry.segment_id}
          if old = @segments.delete(key)
            old.release_lease
          end
          @segment_paths.delete(key)
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

        idx_entry = LogIndexEntry.new(
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
        write_index_entry_unsafe(idx_entry)
        if in_entry.command.conf_change? && !payload.empty?
          apply_conf_change_to_list(@members, payload)
          save_meta!
          {idx_entry, true}
        else
          {idx_entry, false}
        end
      end

      # Standard Raft AppendEntries log consistency check + conflict truncation + append.
      # ameba:disable Metrics/CyclomaticComplexity
      def maybe_append(prev_log_lsn : UInt64,
                       prev_log_term : UInt64,
                       leader_commit : UInt64,
                       entries : Array(LogEntryRecord)) : Tuple(Bool, UInt64, UInt64, UInt64)
        truncated : Array(LogIndexEntry)? = nil
        res, tcb, cb, members_snapshot = @lock.synchronize do
          if prev_log_lsn > @last_lsn
            next { {false, @last_lsn, @last_lsn + 1, 0_u64}, nil, nil, nil }
          end

          if prev_log_lsn > 0
            local_prev_term = term_at(prev_log_lsn)
            if local_prev_term.nil?
              next { {false, @last_lsn, @first_lsn, 0_u64}, nil, nil, nil }
            elsif local_prev_term != prev_log_term
              conflict_term = local_prev_term
              conflict_lsn = prev_log_lsn
              while conflict_lsn > @first_lsn && term_at_unsafe(conflict_lsn - 1) == conflict_term
                conflict_lsn -= 1
              end
              next { {false, @last_lsn, conflict_lsn, conflict_term}, nil, nil, nil }
            end
          end

          membership_changed = false
          log_mutated = false
          entries.each do |rec|
            existing_term = term_at(rec.lsn)
            if existing_term && existing_term != rec.term
              truncated ||= Array(LogIndexEntry).new
              membership_changed |= truncate_from_unsafe(rec.lsn, truncated)
              _, cc_changed = append_follower_record_unsafe(rec)
              membership_changed |= cc_changed
              log_mutated = true
            elsif existing_term.nil?
              _, cc_changed = append_follower_record_unsafe(rec)
              membership_changed |= cc_changed
              log_mutated = true
            end
          end

          last_new_lsn = prev_log_lsn + entries.size.to_u64
          if leader_commit > @commit_index
            new_commit = Math.min(leader_commit, last_new_lsn)
            if new_commit > @commit_index
              membership_changed |= advance_commit_index_unsafe(new_commit)
              log_mutated = true
            end
          end

          fsync_unsafe if log_mutated && @sync

          if membership_changed
            { {true, last_new_lsn, 0_u64, 0_u64}, @on_truncate, @on_membership_change, @members.dup }
          else
            { {true, last_new_lsn, 0_u64, 0_u64}, @on_truncate, nil, nil }
          end
        end
        if tcb && (trunc_list = truncated)
          trunc_list.each { |e| tcb.call(e) }
        end
        if cb && members_snapshot
          cb.call(members_snapshot)
        end
        res
      end

      def first_uncommitted_conf_change_lsn : UInt64?
        @lock.synchronize do
          return if @last_lsn <= @commit_index || @last_lsn < @first_lsn
          lsn = Math.max(@first_lsn, @commit_index + 1)
          while lsn <= @last_lsn
            raw = entry_ptr(lsn).value
            return lsn if raw.lsn == lsn && raw.command.conf_change?
            lsn += 1
          end
          nil
        end
      end

      def advance_commit_index(new_commit : UInt64) : Bool
        cb, members_snapshot = @lock.synchronize do
          return false if new_commit <= @commit_index || new_commit > @last_lsn
          if advance_commit_index_unsafe(new_commit)
            {@on_membership_change, @members.dup}
          else
            {nil, nil}
          end
        end
        if cb && members_snapshot
          cb.call(members_snapshot)
        end
        true
      end

      def advance_applied_index(new_applied : UInt64) : Bool
        @lock.synchronize do
          return false if new_applied <= @applied_index
          @applied_index = new_applied
          write_idx_header_unsafe
          true
        end
      end

      private def advance_commit_index_unsafe(new_commit : UInt64) : Bool
        old_commit = @commit_index
        @commit_index = new_commit
        conf_committed = false
        if @last_lsn >= @first_lsn && new_commit > old_commit
          lsn = Math.max(@first_lsn, old_commit + 1)
          limit = Math.min(@last_lsn, new_commit)
          while lsn <= limit
            raw = entry_ptr(lsn).value
            if raw.lsn == lsn && raw.command.conf_change?
              payload = resolve_payload_unsafe(raw)
              unless payload.empty?
                apply_conf_change_to_list(@committed_members, payload)
                conf_committed = true
              end
            end
            lsn += 1
          end
        end
        save_meta! if conf_committed
        write_idx_header_unsafe
        conf_committed
      end

      # Truncates uncommitted entries from `from_lsn` through `@last_lsn` inclusive.
      # Never truncates committed entries (`from_lsn <= @commit_index` raises).
      def truncate_from(from_lsn : UInt64) : Nil
        truncated = Array(LogIndexEntry).new
        tcb, cb, members_snapshot = @lock.synchronize do
          changed = truncate_from_unsafe(from_lsn, truncated)
          if changed
            {@on_truncate, @on_membership_change, @members.dup}
          else
            {@on_truncate, nil, nil}
          end
        end
        if tcb
          truncated.each { |e| tcb.call(e) }
        end
        if cb && members_snapshot
          cb.call(members_snapshot)
        end
      end

      private def truncate_from_unsafe(from_lsn : UInt64, truncated_out : Array(LogIndexEntry)? = nil) : Bool
        return false if from_lsn > @last_lsn
        if from_lsn <= @commit_index
          raise ArgumentError.new("Cannot truncate committed entry lsn=#{from_lsn} (commit_index=#{@commit_index})")
        end

        conf_truncated = false
        lsn = @last_lsn
        while lsn >= from_lsn
          raw = entry_ptr(lsn).value
          conf_truncated = true if raw.command.conf_change?
          entry = normalize_entry_unsafe(raw)
          truncated_out << entry if truncated_out
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
        if conf_truncated
          recompute_members_unsafe
          save_meta!
        end
        write_idx_header_unsafe
        conf_truncated
      end

      # Returns the normalized 36-byte index entry at `lsn` without reading or copying payload bytes.
      def read_index_entry(lsn : UInt64) : LogIndexEntry?
        @lock.synchronize do
          return if lsn < @first_lsn || lsn > @last_lsn
          raw_entry = entry_ptr(lsn).value
          return if raw_entry.lsn != lsn
          normalize_entry_unsafe(raw_entry)
        end
      end

      # Reads a single log entry at `lsn`.
      # If the entry is a Publish or Ack for a segment that has already been deleted,
      # returns a Tombstone entry so followers can skip obsolete segment bytes without a snapshot.
      def read_entry(lsn : UInt64) : LogEntryRecord?
        @lock.synchronize do
          read_entry_unsafe(lsn, nil)[0]?
        end
      end

      # Reads a single log entry at `lsn` and acquires an `MFile#lease` on any backing mmap'd file
      # so the caller can read the zero-copy payload slice outside `@lock` and release it in `ensure`.
      def read_entry_leased(lsn : UInt64) : Tuple(LogEntryRecord, MFile?)?
        @lock.synchronize do
          rec, mfile = read_entry_unsafe(lsn, true)
          return unless rec
          {rec, mfile}
        end
      end

      private def read_entry_unsafe(lsn : UInt64, lease : Bool?) : Tuple(LogEntryRecord?, MFile?)
        return {nil, nil} if lsn < @first_lsn || lsn > @last_lsn
        raw_entry = entry_ptr(lsn).value
        return {nil, nil} if raw_entry.lsn != lsn
        entry = normalize_entry_unsafe(raw_entry)

        if entry.command.publish? || entry.command.ack?
          key = {entry.vhost_id, entry.queue_id, entry.segment_id}
          if (del_lsn = @deleted_segments[key]?) && del_lsn > lsn
            return {make_tombstone_record(entry), nil}
          end
        end

        payload, leased_mfile = resolve_payload_with_lease_unsafe(raw_entry, !lease.nil?)
        if entry.command.publish? && entry.byte_size > 0 && payload.empty?
          leased_mfile.try(&.release_lease)
          return {make_tombstone_record(entry), nil}
        end
        {LogEntryRecord.new(entry, payload), leased_mfile}
      end

      private def make_tombstone_record(entry : LogIndexEntry) : LogEntryRecord
        tombstone = LogIndexEntry.new(
          term: entry.term,
          lsn: entry.lsn,
          vhost_id: entry.vhost_id,
          cmd_type: LogCommandType::Tombstone.value,
          flags: LogIndexEntry::FLAG_TOMBSTONED | (entry.flags & LogIndexEntry::FLAG_FROM_LEADER),
          queue_id: entry.queue_id,
          segment_id: entry.segment_id,
          position: entry.position,
          byte_size: 0_u32
        )
        LogEntryRecord.new(tombstone, Bytes.empty)
      end

      def read_entries(from_lsn : UInt64, max_count : Int32 = 1024, max_bytes : Int32 = 4 * 1024 * 1024) : Array(LogEntryRecord)?
        @lock.synchronize do
          return Array(LogEntryRecord).new if from_lsn > @last_lsn
          return if from_lsn < @first_lsn

          result = Array(LogEntryRecord).new
          bytes_accum = 0
          lsn = from_lsn
          while lsn <= @last_lsn && result.size < max_count
            rec, _ = read_entry_unsafe(lsn, nil)
            return unless rec
            result << rec
            bytes_accum += sizeof(LogIndexEntry) + rec.payload.size
            lsn += 1
            break if bytes_accum >= max_bytes
          end
          result
        end
      end

      # Reads a batch of log entries starting at `from_lsn` and leases all backing `MFile`s
      # referenced by the returned zero-copy payload slices. Caller MUST release all returned
      # `MFile` leases in an `ensure` block.
      def read_entries_leased(from_lsn : UInt64, max_count : Int32 = 1024, max_bytes : Int32 = 4 * 1024 * 1024) : Tuple(Array(LogEntryRecord), Array(MFile))?
        @lock.synchronize do
          if from_lsn > @last_lsn
            return {Array(LogEntryRecord).new, Array(MFile).new}
          end
          return if from_lsn < @first_lsn

          result = Array(LogEntryRecord).new
          leases = Array(MFile).new
          bytes_accum = 0
          lsn = from_lsn
          while lsn <= @last_lsn && result.size < max_count
            rec, mfile = read_entry_unsafe(lsn, true)
            unless rec
              leases.each(&.release_lease)
              return
            end
            if mfile
              if leases.includes?(mfile)
                mfile.release_lease
              else
                leases << mfile
              end
            end
            result << rec
            bytes_accum += sizeof(LogIndexEntry) + rec.payload.size
            lsn += 1
            break if bytes_accum >= max_bytes
          end
          {result, leases}
        end
      end

      # Returns the byte size of `{vhost_id, queue_id, segment_id}` as of `snap_lsn` by finding
      # the earliest `Publish` entry for that segment at `lsn > snap_lsn` and capping `current_size`
      # to its starting `position`. Returns `nil` if the entire segment was created after `snap_lsn`.
      def segment_size_at(vhost_id : UInt64, queue_id : UInt64, segment_id : UInt32, snap_lsn : UInt64, current_size : Int64) : Int64?
        @lock.synchronize do
          return current_size if snap_lsn >= @last_lsn
          start_lsn = Math.max(@first_lsn, snap_lsn + 1)
          min_pos = current_size
          lsn = start_lsn
          while lsn <= @last_lsn
            raw = entry_ptr(lsn).value
            if raw.lsn == lsn && raw.command.publish? &&
               raw.vhost_id == vhost_id && raw.queue_id == queue_id && raw.segment_id == segment_id
              entry = normalize_entry_unsafe(raw)
              pos = entry.position.to_i64
              min_pos = pos if pos < min_pos
            end
            lsn += 1
          end
          return if min_pos <= sizeof(Int32) && current_size > sizeof(Int32)
          min_pos
        end
      end

      # Returns the byte size of `{vhost_id, queue_id, segment_id}`'s ack file as of `snap_lsn`
      # by subtracting 4 bytes per `Ack` entry logged at `lsn > snap_lsn` for that segment.
      def ack_size_at(vhost_id : UInt64, queue_id : UInt64, segment_id : UInt32, snap_lsn : UInt64, current_size : Int64) : Int64
        @lock.synchronize do
          return current_size if snap_lsn >= @last_lsn
          start_lsn = Math.max(@first_lsn, snap_lsn + 1)
          acks_after = 0_i64
          lsn = start_lsn
          while lsn <= @last_lsn
            raw = entry_ptr(lsn).value
            if raw.lsn == lsn && raw.command.ack? &&
               raw.vhost_id == vhost_id && raw.queue_id == queue_id && raw.segment_id == segment_id
              acks_after += 1
            end
            lsn += 1
          end
          Math.max(0_i64, current_size - acks_after * sizeof(UInt32))
        end
      end

      # Resets the log to a snapshot boundary (`last_included_lsn`, `last_included_term`).
      def install_snapshot_state(last_included_lsn : UInt64,
                                 last_included_term : UInt64,
                                 members : Array(MemberInfo)) : Nil
        cb, new_members = @lock.synchronize do
          has_matching_entry = last_included_lsn >= @first_lsn &&
                               last_included_lsn <= @last_lsn &&
                               term_at_unsafe(last_included_lsn) == last_included_term

          @last_snapshot_lsn = last_included_lsn
          @last_snapshot_term = last_included_term
          @snapshot_members = members.dup
          @commit_index = Math.max(@commit_index, last_included_lsn)
          @applied_index = Math.max(@applied_index, last_included_lsn)

          if has_matching_entry && @last_lsn > last_included_lsn
            @first_lsn = last_included_lsn + 1
            recompute_members_unsafe
          else
            @first_lsn = last_included_lsn + 1
            @last_lsn = last_included_lsn
            @committed_members = members.dup
            @members = members.dup
          end
          write_idx_header_unsafe
          save_meta!
          {@on_membership_change, @members.dup}
        end
        cb.try &.call(new_members)
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

      def compact_ctrl! : Nil
        @lock.synchronize do
          compact_ctrl_unsafe
        end
      end

      def fsync : Nil
        @lock.synchronize do
          fsync_unsafe
        end
      end

      private def fsync_unsafe : Nil
        @idx_mfile.fsync unless @idx_mfile.closed?
        @ctrl_mfile.fsync unless @ctrl_mfile.closed?
      end

      def close : Nil
        @lock.synchronize do
          return if @closed
          @closed = true
          @segments.each_value(&.release_lease)
          @segments.clear
          @segment_paths.clear
          @idx_mfile.close(truncate_to_size: false)
          @ctrl_mfile.close
        end
      end

      private def upsert_into_list(list : Array(MemberInfo), member : MemberInfo) : Nil
        if idx = list.index { |m| m.node_id == member.node_id }
          list[idx] = member
        else
          list << member
        end
      end

      private def update_uris_in_list(list : Array(MemberInfo), node_id : UInt64, peer_uri : String, amqp_uri : String, http_uri : String, mqtt_uri : String = "") : Nil
        if idx = list.index { |m| m.node_id == node_id }
          existing = list[idx]
          list[idx] = MemberInfo.new(
            node_id,
            peer_uri.empty? ? existing.peer_uri : peer_uri,
            amqp_uri.empty? ? existing.amqp_uri : amqp_uri,
            http_uri.empty? ? existing.http_uri : http_uri,
            existing.role,
            mqtt_uri.empty? ? existing.mqtt_uri : mqtt_uri
          )
        end
      end

      private def apply_conf_change_to_list(list : Array(MemberInfo), payload : Bytes) : Nil
        cc = ConfChangeCommand.from_slice(payload)
        case cc.change_type
        in .add_learner?
          m = cc.member
          m.role = MemberRole::Learner
          upsert_into_list(list, m)
        in .promote_learner?
          if idx = list.index { |m| m.node_id == cc.member.node_id }
            existing = list[idx]
            existing.role = MemberRole::Voter
            list[idx] = existing
          else
            m = cc.member
            m.role = MemberRole::Voter
            list << m
          end
        in .remove_node?
          list.reject! { |m| m.node_id == cc.member.node_id }
        end
      end

      private def recompute_members_unsafe : Nil
        recomputed = @snapshot_members.dup
        recomputed_committed = @snapshot_members.dup
        if @last_lsn >= @first_lsn
          lsn = @first_lsn
          while lsn <= @last_lsn
            raw = entry_ptr(lsn).value
            if raw.lsn == lsn && raw.command.conf_change?
              payload = resolve_payload_unsafe(raw)
              unless payload.empty?
                apply_conf_change_to_list(recomputed, payload)
                apply_conf_change_to_list(recomputed_committed, payload) if lsn <= @commit_index
              end
            end
            lsn += 1
          end
        end
        @members = recomputed
        @committed_members = recomputed_committed
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
        resolve_payload_with_lease_unsafe(entry, false)[0]
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def resolve_payload_with_lease_unsafe(entry : LogIndexEntry, lease : Bool) : Tuple(Bytes, MFile?)
        return {Bytes.empty, nil} if entry.byte_size == 0

        if entry.has_ctrl_payload?
          if entry.command.publish?
            return {Bytes.empty, nil} if entry.byte_size <= 4
            slice = @ctrl_mfile.to_slice((entry.position + 4).to_i64, (entry.byte_size - 4).to_i64)
            if lease
              @ctrl_mfile.lease
              return {slice, @ctrl_mfile}
            else
              return {slice.dup, nil}
            end
          end
          slice = @ctrl_mfile.to_slice(entry.position.to_i64, entry.byte_size.to_i64)
          if lease
            @ctrl_mfile.lease
            return {slice, @ctrl_mfile}
          else
            return {slice.dup, nil}
          end
        end

        if entry.command.publish?
          key = {entry.vhost_id, entry.queue_id, entry.segment_id}
          if seg_mfile = @segments[key]?
            if !seg_mfile.closed? && entry.position.to_i64 + entry.byte_size.to_i64 <= seg_mfile.size
              slice = seg_mfile.to_slice(entry.position.to_i64, entry.byte_size.to_i64)
              if lease
                seg_mfile.lease
                return {slice, seg_mfile}
              else
                return {slice.dup, nil}
              end
            end
          end
          if @deleted_segments.has_key?(key)
            return {Bytes.empty, nil}
          end
          if (path = @segment_paths[key]?) && File.exists?(path)
            begin
              File.open(path, "r") do |f|
                f.seek(entry.position)
                buf = Bytes.new(entry.byte_size)
                f.read_fully(buf)
                return {buf, nil}
              end
            rescue
              return {Bytes.empty, nil}
            end
          end
          return {Bytes.empty, nil}
        end

        {Bytes.empty, nil}
      end

      private def ensure_ctrl_space_unsafe(needed : Int) : Nil
        if @ctrl_mfile.size + needed > @ctrl_mfile.capacity || @ctrl_mfile.size + needed > UInt32::MAX
          compact_ctrl_unsafe
        end
        if @ctrl_mfile.size + needed > @ctrl_mfile.capacity
          new_cap = Math.max(@ctrl_mfile.capacity * 2, @ctrl_mfile.size + needed + CTRL_INIT_SIZE)
          raise OverflowError.new("raft.ctrl capacity exceeded UInt32::MAX") if new_cap > UInt32::MAX
          path = @ctrl_mfile.path
          @ctrl_mfile.close(truncate_to_size: false)
          @ctrl_mfile = MFile.new(path, new_cap)
        end
      end

      private def compact_ctrl_unsafe : Nil
        live_bytes = 4_i64
        if @last_lsn >= @first_lsn
          lsn = @first_lsn
          while lsn <= @last_lsn
            raw = entry_ptr(lsn).value
            if raw.lsn == lsn && raw.has_ctrl_payload? && raw.byte_size > 0
              live_bytes += raw.byte_size
            end
            lsn += 1
          end
        end

        new_cap = Math.max(CTRL_INIT_SIZE.to_i64, live_bytes * 2)
        tmp_path = File.join(@data_dir, "raft.ctrl.tmp")
        File.delete?(tmp_path)
        new_ctrl = MFile.new(tmp_path, new_cap)
        new_ctrl.write_bytes(IDX_MAGIC, IO::ByteFormat::LittleEndian)

        if @last_lsn >= @first_lsn
          lsn = @first_lsn
          while lsn <= @last_lsn
            ptr = entry_ptr(lsn)
            raw = ptr.value
            if raw.lsn == lsn && raw.has_ctrl_payload? && raw.byte_size > 0
              new_pos = new_ctrl.size.to_u32
              slice = @ctrl_mfile.to_slice(raw.position.to_i64, raw.byte_size.to_i64)
              new_ctrl.write(slice)
              ptr.value = LogIndexEntry.new(
                term: raw.term,
                lsn: raw.lsn,
                vhost_id: raw.vhost_id,
                cmd_type: raw.cmd_type,
                flags: raw.flags,
                queue_id: raw.queue_id,
                segment_id: raw.segment_id,
                position: new_pos,
                byte_size: raw.byte_size
              )
            end
            lsn += 1
          end
        end

        final_path = @ctrl_mfile.path
        new_ctrl.close(truncate_to_size: false)
        @ctrl_mfile.close(truncate_to_size: false)
        FileSystem.durable_rename(tmp_path, final_path)
        @ctrl_mfile = MFile.new(final_path, new_cap)
      end

      private def append_staged_publish_ctrl(seg_pos : UInt32, payload : Bytes) : UInt32
        needed = payload.size + 4
        ensure_ctrl_space_unsafe(needed)
        pos = @ctrl_mfile.size.to_u32
        @ctrl_mfile.write_bytes(seg_pos, IO::ByteFormat::LittleEndian)
        @ctrl_mfile.write(payload)
        pos
      end

      private def append_ctrl_bytes(bytes : Bytes) : UInt32
        ensure_ctrl_space_unsafe(bytes.size)
        pos = @ctrl_mfile.size.to_u32
        @ctrl_mfile.write(bytes)
        pos
      end

      private def write_index_entry_unsafe(entry : LogIndexEntry) : Nil
        lsn = entry.lsn
        if @first_lsn == 0
          @first_lsn = lsn
        end
        while @last_lsn >= @first_lsn && lsn >= @first_lsn + @capacity
          evictable = @first_lsn <= @commit_index && (!@track_applied || @first_lsn <= @applied_index)
          if evictable
            oldest = entry_ptr(@first_lsn).value
            if oldest.lsn == @first_lsn
              @last_snapshot_lsn = oldest.lsn
              @last_snapshot_term = oldest.term
              if oldest.command.conf_change?
                payload = resolve_payload_unsafe(oldest)
                apply_conf_change_to_list(@snapshot_members, payload) unless payload.empty?
              end
            end
            @first_lsn += 1
          else
            grow_index_ring_unsafe
          end
        end
        entry_ptr(lsn).value = entry
        @last_lsn = lsn
        write_idx_header_unsafe
      end

      private def grow_index_ring_unsafe : Nil
        new_cap = @capacity * 2
        new_mask = new_cap - 1
        existing = Array(LogIndexEntry).new
        if @last_lsn >= @first_lsn
          lsn = @first_lsn
          while lsn <= @last_lsn
            existing << entry_ptr(lsn).value
            lsn += 1
          end
        end

        path = @idx_mfile.path
        total_bytes = IDX_HEADER_SIZE + (new_cap.to_i64 * sizeof(LogIndexEntry))
        @idx_mfile.close(truncate_to_size: false)
        @idx_mfile = MFile.new(path, total_bytes)
        @idx_mfile.resize(total_bytes)
        @capacity = new_cap
        @mask = new_mask

        base = (@idx_mfile.to_slice.to_unsafe + IDX_HEADER_SIZE).as(LogIndexEntry*)
        new_cap.times do |i|
          (base + i).value = LogIndexEntry.new
        end
        existing.each do |e|
          next if e.lsn == 0
          slot = (e.lsn - 1) & @mask
          (base + slot).value = e
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
        existed = File.exists?(path) && File.size(path) >= IDX_HEADER_SIZE
        if existed
          hdr_bytes = Bytes.new(IDX_HEADER_SIZE)
          File.open(path, "r", &.read_fully(hdr_bytes))
          magic = IO::ByteFormat::LittleEndian.decode(UInt32, hdr_bytes[0, 4])
          stored_cap = IO::ByteFormat::LittleEndian.decode(UInt64, hdr_bytes[8, 8])
          if magic == IDX_MAGIC && stored_cap >= @capacity && (stored_cap & (stored_cap - 1)) == 0
            @capacity = stored_cap
            @mask = @capacity - 1
            total_bytes = IDX_HEADER_SIZE + (@capacity.to_i64 * sizeof(LogIndexEntry))
            mfile = MFile.new(path, total_bytes)
            mfile.resize(total_bytes)
            @first_lsn = IO::ByteFormat::LittleEndian.decode(UInt64, hdr_bytes[16, 8])
            @last_lsn = IO::ByteFormat::LittleEndian.decode(UInt64, hdr_bytes[24, 8])
            @commit_index = IO::ByteFormat::LittleEndian.decode(UInt64, hdr_bytes[32, 8])
            @applied_index = IO::ByteFormat::LittleEndian.decode(UInt64, hdr_bytes[40, 8])
            return mfile
          end
        end

        total_bytes = IDX_HEADER_SIZE + (@capacity.to_i64 * sizeof(LogIndexEntry))
        mfile = MFile.new(path, total_bytes)
        mfile.resize(total_bytes)
        @first_lsn = @last_snapshot_lsn + 1
        @last_lsn = @last_snapshot_lsn
        @commit_index = @last_snapshot_lsn
        @applied_index = @last_snapshot_lsn
        write_idx_header_to_slice(mfile.to_slice)
        mfile
      end

      private def write_idx_header_unsafe : Nil
        return if @idx_mfile.closed?
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
        IO::ByteFormat::LittleEndian.encode(@applied_index, Slice.new(ptr + 40, 8))
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
              if io.pos < io.size
                snap_count = fmt.decode(UInt32, io)
                @snapshot_members = Array(MemberInfo).new(snap_count.to_i) { MemberInfo.from_io(io, fmt) }
              else
                @snapshot_members = @members.dup
              end
              @committed_members = @members.dup
              @meta_existed = true
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
        fmt.encode(@snapshot_members.size.to_u32, body_io)
        @snapshot_members.each(&.to_io(body_io, fmt))

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
