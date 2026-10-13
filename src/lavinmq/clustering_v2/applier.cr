require "digest/crc32"
require "digest/sha1"
require "file_utils"
require "json"
require "../server"
require "../message_store"
require "../amqp/queue/queue"
require "../amqp/stream/stream"
require "../amqp/stream/stream_message_store"
require "../mqtt/retain_store"
require "./protocol"
require "./raft_log"

module LavinMQ
  class MessageStore
    property? commit_gated : Bool = false
    property committed_segment_id : UInt32 = UInt32::MAX
    property committed_end_pos : UInt32 = UInt32::MAX
    @on_committed_callback : Proc(Nil)? = nil

    def on_committed(&block : -> Nil) : Nil
      @on_committed_callback = block
    end

    def enable_commit_gate! : Nil
      @commit_gated = true
      @committed_segment_id = @wfile_id
      @committed_end_pos = @wfile.size.to_u32
      @empty.set(empty?) unless @closed
    end

    def disable_commit_gate! : Nil
      @commit_gated = false
      @committed_segment_id = UInt32::MAX
      @committed_end_pos = UInt32::MAX
      @empty.set(empty?) unless @closed
    end

    @[AlwaysInline]
    protected def uncommitted_at?(seg : UInt32, pos : UInt32) : Bool
      return false unless @commit_gated
      seg > @committed_segment_id || (seg == @committed_segment_id && pos >= @committed_end_pos)
    end

    protected def has_committed_ready_message? : Bool
      return false if @size.zero?
      return true if @requeued.first?
      return true unless @commit_gated

      loop do
        seg = @rfile_id
        rfile = @rfile
        pos = rfile.pos.to_u32
        if pos == rfile.size
          select_next_read_segment && next
          return false
        end
        if deleted?(seg, pos)
          BytesMessage.skip(rfile)
          next
        end
        return !uncommitted_at?(seg, pos)
      end
    end

    def empty? : Bool
      return true if @size.zero?
      return false unless @commit_gated
      return false if @requeued.first?
      !has_committed_ready_message?
    end

    def push(msg) : SegmentPosition
      return previous_def(msg) unless @commit_gated
      raise ClosedError.new if @closed
      sp = write_to_disk(msg)
      @bytesize += sp.bytesize
      @size += 1
      @empty.set(empty?)
      sp
    end

    def first? : Envelope?
      raise ClosedError.new if @closed
      if @commit_gated && @requeued.first?.nil?
        return unless has_committed_ready_message?
      end
      previous_def
    end

    def shift?(consumer = nil) : Envelope?
      raise ClosedError.new if @closed
      if @commit_gated && @requeued.first?.nil?
        return unless has_committed_ready_message?
      end
      env = previous_def(consumer)
      if env && @commit_gated && !@size.zero?
        @empty.set(true) if empty?
      end
      env
    end

    def advance_committed(seg_id : UInt32, end_pos : UInt32) : Nil
      return if @closed
      if @committed_segment_id == UInt32::MAX ||
         seg_id > @committed_segment_id ||
         (seg_id == @committed_segment_id && end_pos > @committed_end_pos)
        @committed_segment_id = seg_id
        @committed_end_pos = end_pos
      end
      if @commit_gated && !empty?
        @empty.set(false)
        @on_committed_callback.try &.call
      end
    end

    def current_segment_mfile(seg_id : UInt32) : MFile?
      @segments[seg_id]?
    end

    def apply_follower_publish(seg_id : UInt32, position : UInt32, msg_bytes : Bytes) : MFile
      raise ClosedError.new if @closed
      while @wfile_id < seg_id
        open_new_segment(msg_bytes.size)
      end
      mfile = @segments[seg_id]? || begin
        path = File.join(@msg_dir, "msgs.#{seg_id.to_s.rjust(10, '0')}")
        capacity = Math.max(Config.instance.segment_size, position.to_i + msg_bytes.size + 4)
        f = open_segment(path, capacity)
        if f.size.zero?
          f.write_bytes Schema::VERSION
        end
        f.pos = 4
        @segments[seg_id] = f
        if seg_id > @wfile_id
          @wfile_id = seg_id
          @wfile = f
        end
        f
      end
      if mfile.size != position
        mfile.resize(position)
      end
      mfile.write(msg_bytes)
      was_empty = @size.zero?
      @bytesize += msg_bytes.size.to_u64
      @size += 1
      @segment_msg_count[seg_id] += 1
      if @committed_segment_id != UInt32::MAX
        @committed_segment_id = seg_id
        @committed_end_pos = (position + msg_bytes.size).to_u32
      end
      @empty.set(false) if was_empty && !@commit_gated
      mfile
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def apply_follower_ack(seg_id : UInt32, position : UInt32) : Nil
      return if @closed
      mfile = @segments[seg_id]? || return
      return if seg_id < @rfile_id
      return if seg_id == @rfile_id && position < @rfile.pos.to_u32
      return if deleted?(seg_id, position)
      return if position >= mfile.size

      msg = BytesMessage.from_bytes(mfile.to_slice + position)
      msg_bytesize = msg.bytesize.to_u32

      afile = @acks[seg_id]
      afile.write_bytes(position)

      if seg_id == @rfile_id && position == @rfile.pos.to_u32
        @rfile.seek(msg_bytesize, IO::Seek::Current)
        drain_contiguous_deleted_at_rfile
      else
        del = (@deleted[seg_id] ||= Array(UInt32).new)
        if idx = del.bsearch_index { |dpos| dpos >= position }
          del.insert(idx, position) unless del[idx] == position
        else
          del << position
        end
      end

      @bytesize = @bytesize >= msg_bytesize ? @bytesize - msg_bytesize : 0_u64
      @size -= 1 if @size > 0
      @empty.set(empty?)

      return if seg_id == @wfile_id
      ack_count = afile.size // sizeof(UInt32)
      msg_count = @segment_msg_count[seg_id]
      if ack_count >= msg_count && msg_count > 0
        select_next_read_segment if seg_id == @rfile_id
        if a = @acks.delete(seg_id)
          delete_file(a)
        end
        if seg = @segments.delete(seg_id)
          delete_file(seg, including_meta: true)
        end
        @segment_msg_count.delete(seg_id)
        @deleted.delete(seg_id)
        drain_contiguous_deleted_at_rfile
      end
    end

    private def drain_contiguous_deleted_at_rfile : Nil
      loop do
        if del = @deleted[@rfile_id]?
          while @rfile.pos < @rfile.size
            pos = @rfile.pos.to_u32
            idx = del.bsearch_index { |dpos| dpos >= pos }
            break unless idx && del[idx] == pos
            del.delete_at(idx)
            BytesMessage.skip(@rfile)
          end
          @deleted.delete(@rfile_id) if del.empty?
        end
        if @rfile.pos == @rfile.size && @rfile_id < @wfile_id
          select_next_read_segment || break
        else
          break
        end
      end
    end

    # ameba:disable Metrics/CyclomaticComplexity
    def apply_follower_segment_delete(seg_id : UInt32) : Nil
      return if @closed
      mfile = @segments[seg_id]? || return
      return if seg_id == @wfile_id

      if seg_id >= @rfile_id
        # Subtract any remaining unacknowledged messages in this segment
        pos = seg_id == @rfile_id ? @rfile.pos.to_u32 : 4_u32
        while pos < mfile.size
          msg = BytesMessage.from_bytes(mfile.to_slice + pos)
          bs = msg.bytesize.to_u32
          unless deleted?(seg_id, pos)
            @bytesize = @bytesize >= bs ? @bytesize - bs : 0_u64
            @size -= 1 if @size > 0
          end
          pos += bs
        end
      end

      select_next_read_segment if seg_id == @rfile_id
      if a = @acks.delete(seg_id)
        delete_file(a)
      end
      if seg = @segments.delete(seg_id)
        delete_file(seg, including_meta: true)
      end
      @segment_msg_count.delete(seg_id)
      @deleted.delete(seg_id)
      @empty.set(empty?)
    end

    def rollback_uncommitted_publish(seg_id : UInt32, position : UInt32, byte_size : UInt32) : Nil
      return if @closed
      mfile = @segments[seg_id]? || return
      if mfile.size >= position + byte_size
        mfile.resize(position)
      end
      @bytesize = @bytesize >= byte_size ? @bytesize - byte_size : 0_u64
      @size -= 1 if @size > 0
      if (cnt = @segment_msg_count[seg_id]?) && cnt > 0
        @segment_msg_count[seg_id] = cnt - 1
      end
      @empty.set(empty?)
    end
  end

  module AMQP
    class StreamMessageStore < MessageStore
      def shift?(consumer : AMQP::StreamConsumer) : Envelope?
        raise ClosedError.new if @closed
        if @commit_gated && consumer.requeued.empty?
          if uncommitted_at?(consumer.segment, consumer.pos)
            return
          end
        end
        previous_def(consumer)
      end

      def advance_committed(seg_id : UInt32, end_pos : UInt32) : Nil
        super(seg_id, end_pos)
        while @new_messages.try_send?(true)
        end
      end

      def apply_follower_publish(seg_id : UInt32, position : UInt32, msg_bytes : Bytes) : MFile
        @last_offset += 1
        mfile = super(seg_id, position, msg_bytes)
        if msg_bytes.size >= 8
          ts = IO::ByteFormat::SystemEndian.decode(Int64, msg_bytes[0, 8])
          @segment_last_ts[seg_id] = ts
          @segment_first_offset[seg_id] ||= @last_offset
          @segment_first_ts[seg_id] ||= ts
        end
        while @new_messages.try_send?(true)
        end
        mfile
      end

      def apply_follower_stream_offset(consumer_tag : String, offset : Int64) : Nil
        return if @closed
        @consumer_offsets.store(consumer_tag, offset) { lowest_offset_in_stream }
      end

      def rollback_uncommitted_publish(seg_id : UInt32, position : UInt32, byte_size : UInt32) : Nil
        super(seg_id, position, byte_size)
        @last_offset -= 1 if @last_offset > 0
      end
    end

    class Queue
      def raw_msg_store : MessageStore
        @msg_store
      end

      def enable_commit_gate! : Nil
        @msg_store_lock.synchronize do
          @msg_store.on_committed { ensure_consumers_deliver_loops }
          @msg_store.enable_commit_gate!
        end
      end

      def disable_commit_gate! : Nil
        @msg_store_lock.synchronize do
          @msg_store.disable_commit_gate!
        end
        ensure_consumers_deliver_loops unless @msg_store.empty?
      end

      def advance_committed(seg_id : UInt32, end_pos : UInt32) : Nil
        @msg_store_lock.synchronize do
          @msg_store.advance_committed(seg_id, end_pos)
        end
        ensure_consumers_deliver_loops unless @msg_store.empty?
      end

      def apply_follower_publish(seg_id : UInt32, position : UInt32, msg_bytes : Bytes) : MFile
        mfile = @msg_store_lock.synchronize do
          @msg_store.apply_follower_publish(seg_id, position, msg_bytes)
        end
        @publish_count.add(1, :relaxed)
        ensure_consumers_deliver_loops unless @msg_store.empty?
        mfile
      end

      def apply_follower_ack(seg_id : UInt32, position : UInt32) : Nil
        @msg_store_lock.synchronize do
          @msg_store.apply_follower_ack(seg_id, position)
        end
        @ack_count.add(1, :relaxed)
      end

      def apply_follower_segment_delete(seg_id : UInt32) : Nil
        @msg_store_lock.synchronize do
          @msg_store.apply_follower_segment_delete(seg_id)
        end
      end

      def apply_follower_stream_offset(consumer_tag : String, offset : Int64) : Nil
        @msg_store_lock.synchronize do
          if store = @msg_store.as?(StreamMessageStore)
            store.apply_follower_stream_offset(consumer_tag, offset)
          end
        end
      end

      def rollback_uncommitted_publish(seg_id : UInt32, position : UInt32, byte_size : UInt32) : Nil
        @msg_store_lock.synchronize do
          @msg_store.rollback_uncommitted_publish(seg_id, position, byte_size)
        end
      end
    end
  end

  module MQTT
    class Broker
      def raw_retain_store : RetainStore
        @retain_store
      end
    end

    class RetainStore
      def apply_follower_retain(topic : String, payload : Bytes) : Nil
        pkt = Protocol::Publish.new(
          topic: topic,
          payload: payload,
          packet_id: nil,
          qos: 0_u8,
          retain: true,
          dup: false
        )
        retain(pkt)
      end

      def apply_follower_delete_file(file_name : String) : Nil
        @lock.synchronize do
          matched_topic = nil
          @index.each do |topic, fn|
            matched_topic = topic if fn == file_name
          end
          if topic = matched_topic
            delete_from_index(topic)
          else
            @files.delete(file_name).try do |f|
              f.close
              f.delete
            end
            File.delete?(File.join(@dir, file_name))
          end
        end
      end
    end
  end

  module ClusteringV2
    module ResourceId
      extend self

      def vhost_id_for_dir(vhost_dir : String) : UInt16
        # Digest::SHA1.hexdigest("/") == "42099b4af021e53fd8fd4e056c2568d7c2e3ffa8"
        return 1_u16 if vhost_dir == "42099b4af021e53fd8fd4e056c2568d7c2e3ffa8"
        raw = (Digest::CRC32.checksum(vhost_dir) & 0xFFFF_u32).to_u16
        raw <= 1_u16 ? (raw + 2_u16) : raw
      end

      def vhost_id_for_name(vhost_name : String) : UInt16
        return 1_u16 if vhost_name == "/"
        vhost_id_for_dir(Digest::SHA1.hexdigest(vhost_name))
      end

      def queue_id_for_dirs(vhost_dir : String, queue_dir : String) : UInt32
        raw = Digest::CRC32.checksum("#{vhost_dir}/#{queue_dir}")
        raw.zero? ? 1_u32 : raw
      end

      def queue_id_for_names(vhost_name : String, queue_name : String) : UInt32
        queue_id_for_dirs(Digest::SHA1.hexdigest(vhost_name), Digest::SHA1.hexdigest(queue_name))
      end
    end

    # Applies committed Raft log entries to the Hot-Standby LavinMQ::Server
    # on followers, advances commit-gated delivery watermarks on the leader,
    # and rolls back uncommitted message store appends on leader step-down.
    class Applier
      Log = LavinMQ::Log.for "clustering_v2.applier"

      property server : LavinMQ::Server?
      getter raft_log : RaftLog
      @suppress_callback : Proc(Bool, Nil)?

      def initialize(@raft_log : RaftLog, @server : LavinMQ::Server? = nil, &suppress_callback : Bool -> Nil)
        @suppress_callback = suppress_callback
      end

      def initialize(@raft_log : RaftLog, @server : LavinMQ::Server? = nil)
        @suppress_callback = nil
      end

      private def with_suppressed_replication(&)
        @suppress_callback.try &.call(true)
        begin
          yield
        ensure
          @suppress_callback.try &.call(false)
        end
      end

      def resolve_vhost(vhost_id : UInt16) : VHost?
        srv = @server || return
        srv.vhosts.each_value do |vh|
          return vh if ResourceId.vhost_id_for_dir(vh.dir) == vhost_id
        end
        nil
      end

      def resolve_queue(vhost_id : UInt16, queue_id : UInt32) : AMQP::Queue?
        vh = resolve_vhost(vhost_id) || return
        vh.queues.each do |q|
          return q if ResourceId.queue_id_for_dirs(vh.dir, Digest::SHA1.hexdigest(q.name)) == queue_id
        end
        nil
      end

      def register_all_segments! : Nil
        srv = @server || return
        srv.vhosts.each_value do |vh|
          vh_id = ResourceId.vhost_id_for_dir(vh.dir)
          vh.queues.each do |q|
            next unless q.durable?
            q_id = ResourceId.queue_id_for_dirs(vh.dir, Digest::SHA1.hexdigest(q.name))
            register_queue_segments(vh_id, q_id, q)
          end
        end
      end

      private def register_queue_segments(vhost_id : UInt16, queue_id : UInt32, q : AMQP::Queue) : Nil
        store = q.raw_msg_store
        1_u32.upto(1024_u32) do |seg_id|
          if mfile = store.current_segment_mfile(seg_id)
            @raft_log.register_segment(vhost_id, queue_id, seg_id, mfile)
          end
        end
      end

      # Called when `record` is committed in Raft.
      # - If `from_leader` is true (this node proposed and already executed the mutation locally),
      #   we only need to advance the commit-gated consumer delivery watermark on `Publish`.
      # - If `from_leader` is false (this node is a Hot-Standby Follower), we apply the command
      #   directly to the live in-memory `LavinMQ::Server` state machine.
      # ameba:disable Metrics/CyclomaticComplexity
      def apply_committed(record : LogEntryRecord, from_leader : Bool) : Nil
        entry = record.entry
        if from_leader
          if entry.command == LogCommandType::Publish
            if q = resolve_queue(entry.vhost_id, entry.queue_id)
              q.advance_committed(entry.segment_id, entry.position + entry.byte_size)
            end
          end
          return
        end

        with_suppressed_replication do
          case entry.command
          when LogCommandType::LeaderNoOp, LogCommandType::ConfChange, LogCommandType::Tombstone
            # Handled by RaftNode
          when LogCommandType::VHostCreate
            apply_vhost_create(record.payload)
          when LogCommandType::VHostDelete
            apply_vhost_delete(record.payload)
          when LogCommandType::QueueDeclare,
               LogCommandType::QueueDelete,
               LogCommandType::ExchangeDeclare,
               LogCommandType::ExchangeDelete,
               LogCommandType::BindingAdd,
               LogCommandType::BindingRemove
            apply_definition_frame(entry.vhost_id, record.payload)
          when LogCommandType::QueuePurge
            if q = resolve_queue(entry.vhost_id, entry.queue_id)
              q.purge
            end
          when LogCommandType::Publish
            if q = resolve_queue(entry.vhost_id, entry.queue_id)
              mfile = q.apply_follower_publish(entry.segment_id, entry.position, record.payload)
              @raft_log.register_segment(entry.vhost_id, entry.queue_id, entry.segment_id, mfile)
              @raft_log.bind_publish_segment(entry.lsn, entry.segment_id, entry.position, record.payload.size.to_u32)
            end
          when LogCommandType::Ack
            if q = resolve_queue(entry.vhost_id, entry.queue_id)
              q.apply_follower_ack(entry.segment_id, entry.position)
            end
          when LogCommandType::SegmentDelete
            if q = resolve_queue(entry.vhost_id, entry.queue_id)
              q.apply_follower_segment_delete(entry.segment_id)
            end
          when LogCommandType::StreamOffsetCommit
            if q = resolve_queue(entry.vhost_id, entry.queue_id)
              io = IO::Memory.new(record.payload)
              tag = AMQ::Protocol::ShortString.from_io(io)
              offset = Int64.from_io(io, IO::ByteFormat::LittleEndian)
              q.apply_follower_stream_offset(tag, offset)
            end
          when LogCommandType::MqttRetainPut
            if vh = resolve_vhost(entry.vhost_id)
              io = IO::Memory.new(record.payload)
              topic_len = UInt16.from_io(io, IO::ByteFormat::LittleEndian)
              topic = io.read_string(topic_len)
              payload_len = UInt32.from_io(io, IO::ByteFormat::LittleEndian)
              payload = Bytes.new(payload_len)
              io.read_fully(payload)
              vh.mqtt_broker.raw_retain_store.apply_follower_retain(topic, payload)
            end
          when LogCommandType::MqttRetainDelete
            if vh = resolve_vhost(entry.vhost_id)
              file_name = String.new(record.payload)
              vh.mqtt_broker.raw_retain_store.apply_follower_delete_file(file_name)
            end
          end
        end
      end

      def rollback_uncommitted(entry : LogIndexEntry) : Nil
        return unless entry.command == LogCommandType::Publish
        with_suppressed_replication do
          if q = resolve_queue(entry.vhost_id, entry.queue_id)
            q.rollback_uncommitted_publish(entry.segment_id, entry.position, entry.byte_size)
          end
        end
      end

      private def apply_vhost_create(payload : Bytes) : Nil
        srv = @server || return
        json = JSON.parse(String.new(payload))
        if arr = json.as_a?
          arr.each do |vh_obj|
            name = vh_obj["name"].as_s
            description = vh_obj["description"]?.try(&.as_s) || ""
            tags = vh_obj["tags"]?.try(&.as_a.map(&.as_s)) || Array(String).new
            srv.vhosts.create(name, description: description, tags: tags) unless srv.vhosts[name]?
          end
        end
      end

      private def apply_vhost_delete(payload : Bytes) : Nil
        srv = @server || return
        dir_or_name = String.new(payload)
        target_name = nil
        srv.vhosts.each_value do |vh|
          if vh.dir == dir_or_name || vh.name == dir_or_name
            target_name = vh.name
            break
          end
        end
        if name = target_name
          srv.vhosts.delete(name)
        end
      end

      private def apply_definition_frame(vhost_id : UInt16, payload : Bytes) : Nil
        vh = resolve_vhost(vhost_id) || return
        io = IO::Memory.new(payload)
        stream = AMQ::Protocol::Stream.new(io, format: IO::ByteFormat::SystemEndian)
        frame = stream.next_frame
        vh.apply(frame, loading: false, fsync: false)
        if frame.is_a?(AMQP::Frame::Queue::Declare)
          if q = vh.queue?(frame.queue_name)
            q_id = ResourceId.queue_id_for_dirs(vh.dir, Digest::SHA1.hexdigest(q.name))
            register_queue_segments(vhost_id, q_id, q)
          end
        end
      end

      def install_snapshot(req : InstallSnapshotRequest) : Nil
        srv = @server || return
        with_suppressed_replication do
          # Close durable queues and clear old segments before writing snapshot segments
          srv.vhosts.each_value do |vh|
            vh.queues.each do |q|
              next unless q.durable?
              q.close
              q_dir = File.join(vh.data_dir, Digest::SHA1.hexdigest(q.name))
              if Dir.exists?(q_dir)
                Dir.each_child(q_dir) do |child|
                  if child.starts_with?("msgs.") || child.starts_with?("acks.") || child == "consumer_offsets"
                    File.delete?(File.join(q_dir, child))
                  end
                end
              end
            end
          end

          req.files.each do |seg_file|
            full_path = File.join(srv.data_dir, seg_file.relative_path)
            FileUtils.mkdir_p(File.dirname(full_path))
            File.write(full_path, seg_file.bytes)
          end

          # Restart queues whose segments were replaced by snapshot so their MFiles map the snapshot
          srv.vhosts.each_value do |vh|
            vh_id = ResourceId.vhost_id_for_dir(vh.dir)
            vh.queues.each do |q|
              next unless q.durable?
              q.restart!
              q_id = ResourceId.queue_id_for_dirs(vh.dir, Digest::SHA1.hexdigest(q.name))
              register_queue_segments(vh_id, q_id, q)
            end
          end
        end
      end
    end
  end
end
