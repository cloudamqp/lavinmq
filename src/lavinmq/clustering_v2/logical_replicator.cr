require "digest/md5"
require "../clustering/replicator"
require "../clustering/follower"
require "./protocol"
require "./raft_log"
require "./raft_node"
require "./applier"

module LavinMQ
  module ClusteringV2
    # Implements `LavinMQ::Clustering::Replicator` on top of the embedded Raft
    # consensus engine (`RaftNode`), zero-copy segment index (`RaftLog`), and
    # Hot-Standby state-machine `Applier`.
    class LogicalReplicator
      include LavinMQ::Clustering::Replicator

      Log = LavinMQ::Log.for "clustering_v2.replicator"

      getter node : RaftNode
      getter log : RaftLog
      getter applier : Applier
      getter server : LavinMQ::Server?
      property? commit_gated_delivery : Bool = true
      property? suppressed : Bool = false
      property wait_timeout : Time::Span = 5.seconds

      private enum SegmentFileKind : UInt8
        Msgs            = 1
        Acks            = 2
        ConsumerOffsets = 3
      end

      private struct SegmentPathInfo
        getter vhost_id : UInt64
        getter queue_id : UInt64
        getter seg_id : UInt32
        getter kind : SegmentFileKind
        property? gate_initialized : Bool = false

        def initialize(@vhost_id : UInt64, @queue_id : UInt64, @seg_id : UInt32, @kind : SegmentFileKind, @gate_initialized : Bool = false)
        end
      end

      @data_dir : String
      @segment_path_cache = Hash(String, SegmentPathInfo).new
      @mqtt_file_to_topic = Hash(String, String).new
      @last_proposed_lsn = Atomic(UInt64).new(0_u64)
      @lock = Mutex.new(:reentrant)
      @on_role_change : Proc(Role, Nil)?
      @suppressing_fiber : Fiber? = nil
      @suppress_depth : Int32 = 0

      def initialize(@data_dir : String,
                     @node : RaftNode,
                     @commit_gated_delivery : Bool = true)
        @log = @node.log
        @applier = Applier.new(@log) do |suppress|
          if suppress
            @suppressing_fiber = Fiber.current
            @suppress_depth += 1
          else
            @suppress_depth -= 1 if @suppress_depth > 0
            @suppressing_fiber = nil if @suppress_depth.zero?
          end
        end

        @node.on_truncate do |entry|
          @applier.rollback_uncommitted(entry)
        end

        @node.on_apply do |rec|
          @applier.apply_committed(rec, from_leader: rec.entry.from_leader?)
        end

        @node.on_promote do |_term|
          handle_promotion
        end

        @node.on_step_down do |_term|
          handle_step_down
        end

        @node.on_build_snapshot do |snap_lsn|
          build_snapshot_segments(snap_lsn)
        end

        @node.on_install_snapshot do |req|
          @applier.install_snapshot(req)
        end
      end

      @[AlwaysInline]
      private def replication_suppressed? : Bool
        @suppressed || @suppressing_fiber == Fiber.current
      end

      def standby? : Bool
        !@node.promoted?
      end

      def on_role_change(&block : Role -> Nil) : Nil
        @on_role_change = block
      end

      def attach(server : LavinMQ::Server) : Nil
        @server = server
        @applier.server = server
        @applier.register_all_segments!
        if @node.promoted?
          enable_commit_gates! if @commit_gated_delivery
          server.on_leader_promotion
        end
      end

      def enable_commit_gates! : Nil
        srv = @server || return
        srv.vhosts.each_value do |vh|
          vh.queues.each do |q|
            q.enable_commit_gate! if q.durable?
          end
        end
      end

      def disable_commit_gates! : Nil
        srv = @server || return
        srv.vhosts.each_value do |vh|
          vh.queues.each do |q|
            q.disable_commit_gate! if q.durable?
          end
        end
      end

      private def handle_promotion : Nil
        if @commit_gated_delivery
          enable_commit_gates!
        else
          disable_commit_gates!
        end
        if srv = @server
          Fiber::ExecutionContext.default.spawn(name: "ClusteringV2 leader promotion") do
            srv.on_leader_promotion
          end
        end
        @on_role_change.try &.call(Role::Leader)
      end

      # Clean in-process step-down without terminating the process (`exit 3` is eliminated).
      # Dispatches connection closing to the default execution context so the Raft
      # or Persister execution contexts are never blocked by client socket writes/handshakes.
      private def handle_step_down : Nil
        disable_commit_gates!
        @on_role_change.try &.call(@node.role)
        if srv = @server
          Fiber::ExecutionContext.default.spawn(name: "ClusteringV2 leader step-down") do
            srv.on_leader_step_down
            srv.vhosts.each_value do |vh|
              vh.connections.dup.each do |conn|
                Fiber::ExecutionContext.default.spawn(name: "ClusteringV2 step-down close client") do
                  conn.close("Stepping down from cluster leader") rescue nil
                end
              end
            end
          end
        end
      end

      private def build_snapshot_segments(snap_lsn : UInt64) : Array(SnapshotSegmentFile)
        result = Array(SnapshotSegmentFile).new
        srv = @server || return result

        {"vhosts.json", "users.json", "parameters.json"}.each do |fn|
          full = File.join(srv.data_dir, fn)
          if bytes = @applier.committed_file_bytes(fn, full)
            result << SnapshotSegmentFile.new("", "", 0_u32, fn, bytes)
          end
        end

        srv.vhosts.each_value do |vh|
          vh_id = ResourceId.vhost_id_for_dir(vh.dir)
          {".vhost", "definitions.amqp", "limits.json", "policies.json", "operator_policies.json", "parameters.json", "mqtt_permissions.json"}.each do |fn|
            rel = "#{vh.dir}/#{fn}"
            full = File.join(srv.data_dir, rel)
            if bytes = @applier.committed_file_bytes(rel, full)
              result << SnapshotSegmentFile.new(vh.name, "", 0_u32, rel, bytes)
            end
          end

          retain_dir = File.join(vh.data_dir, "mqtt_retained_store")
          if Dir.exists?(retain_dir)
            Dir.each_child(retain_dir) do |child|
              next unless child == "index" || child.ends_with?(".msg")
              full = File.join(retain_dir, child)
              next unless File.file?(full)
              rel = "#{vh.dir}/mqtt_retained_store/#{child}"
              result << SnapshotSegmentFile.new(vh.name, "", 0_u32, rel, File.read(full).to_slice.dup)
            end
          end

          vh.queues.each do |q|
            next unless q.durable?
            q_dir_name = q.queue_dir_name
            q_id = ResourceId.queue_id_for_dirs(vh.dir, q_dir_name)
            {".queue", ".meta"}.each do |fn|
              rel = "#{vh.dir}/#{q_dir_name}/#{fn}"
              full = File.join(srv.data_dir, rel)
              if File.exists?(full)
                result << SnapshotSegmentFile.new(vh.name, q.name, 0_u32, rel, File.read(full).to_slice.dup)
              end
            end
            q.snapshot_segments(vh.dir, q_dir_name, vh_id, q_id, snap_lsn, @log, result)
          end
        end
        result
      end

      # --- LavinMQ::Clustering::Replicator interface ---

      def register_file(path : String)
        if info = cache_segment_path_info(path)
          if info.kind.msgs?
            @log.register_segment_path(info.vhost_id, info.queue_id, info.seg_id, path)
          end
        end
      end

      def register_file(file : File)
      end

      def register_file(mfile : MFile)
        if info = cache_segment_path_info(mfile.path)
          if info.kind.msgs?
            @log.register_segment(info.vhost_id, info.queue_id, info.seg_id, mfile)
          end
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def replace_file(path : String)
        return if replication_suppressed? || @server.nil?
        rel = relative_path(path)
        parts = rel.split('/')
        is_mgmt_file = (parts.size == 1 && parts[0].in?("vhosts.json", "users.json", "parameters.json")) ||
                       (parts.size == 2 && parts[1].in?("parameters.json", "policies.json", "operator_policies.json", "limits.json", "mqtt_permissions.json"))

        begin
          if parts.size == 1 && parts[0] == "vhosts.json"
            return unless File.exists?(path)
            unless @node.leader?
              @applier.restore_committed_file!(rel)
              raise Clustering::Replicator::ReplicationError.new("Node #{@node.node_id} is not the cluster leader")
            end
            if srv = @server
              srv.vhosts.each_value { |vh| @applier.index_vhost!(vh) }
            end
            payload = File.read(path).to_slice
            entry = @node.propose_command(LogCommandType::VHostCreate, payload)
            @applier.record_pending_file_replace(entry.lsn, rel)
            @last_proposed_lsn.set(entry.lsn)
            wait_for_followers
          elsif is_mgmt_file
            return unless File.exists?(path)
            unless @node.leader?
              @applier.restore_committed_file!(rel)
              raise Clustering::Replicator::ReplicationError.new("Node #{@node.node_id} is not the cluster leader")
            end
            content = File.read(path).to_slice
            rel_bytes = rel.to_slice
            io = IO::Memory.new(2 + rel_bytes.size + 4 + content.size)
            io.write_bytes(rel_bytes.size.to_u16, IO::ByteFormat::LittleEndian)
            io.write(rel_bytes)
            io.write_bytes(content.size.to_u32, IO::ByteFormat::LittleEndian)
            io.write(content)
            vhost_id = parts.size == 2 ? ResourceId.vhost_id_for_dir(parts[0]) : 0_u64
            entry = @node.propose_command(LogCommandType::FileReplace, io.to_slice, vhost_id: vhost_id)
            @applier.record_pending_file_replace(entry.lsn, rel)
            @last_proposed_lsn.set(entry.lsn)
            wait_for_followers
          elsif parts.size == 3 && parts[1] == "mqtt_retained_store" && parts[2].ends_with?(".msg")
            return unless @node.leader? && File.exists?(path)
            vhost_dir = parts[0]
            vhost_id = ResourceId.vhost_id_for_dir(vhost_dir)
            msg_file = parts[2]
            topic = resolve_mqtt_topic(vhost_dir, msg_file) || return
            msg_payload = File.read(path).to_slice

            io = IO::Memory.new(2 + topic.bytesize + 4 + msg_payload.size)
            io.write_bytes(topic.bytesize.to_u16, IO::ByteFormat::LittleEndian)
            io.write(topic.to_slice)
            io.write_bytes(msg_payload.size.to_u32, IO::ByteFormat::LittleEndian)
            io.write(msg_payload)

            topic_id = Digest::CRC32.checksum(msg_file).to_u64
            entry = @node.propose_command(LogCommandType::MqttRetainPut, io.to_slice, vhost_id: vhost_id, queue_id: topic_id)
            @last_proposed_lsn.set(entry.lsn)
          end
        rescue ex : Clustering::Replicator::ReplicationError | RaftNode::Error::NotLeaderError
          if is_mgmt_file
            abort_uncommitted!
            @applier.restore_committed_file!(rel)
            raise Clustering::Replicator::ReplicationError.new(ex.message || "Replication failed")
          end
        end
      end

      def replace_file(mfile : MFile)
        if info = (@lock.synchronize { @segment_path_cache[mfile.path]? } || cache_segment_path_info(mfile.path))
          if info.kind.msgs?
            @log.register_segment(info.vhost_id, info.queue_id, info.seg_id, mfile)
          end
        end
      end

      private def abort_inflight_publish!(info : SegmentPathInfo, position : UInt32) : Nil
        if q = @applier.resolve_queue(info.vhost_id, info.queue_id)
          return if q.abort_inflight_publish_unlocked(info.seg_id, position)
        end
        if mfile = @log.registered_segment(info.vhost_id, info.queue_id, info.seg_id)
          mfile.resize(position) if !mfile.closed? && mfile.size > position
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def append(path : String, pos : Int, length : Int)
        return if replication_suppressed? || @server.nil?
        info = @lock.synchronize { @segment_path_cache[path]? } || cache_segment_path_info(path) || return

        case info.kind
        when .msgs?
          unless @node.leader?
            abort_inflight_publish!(info, pos.to_u32)
            raise Clustering::Replicator::ReplicationError.new("Node #{@node.node_id} is not the cluster leader")
          end
          if @commit_gated_delivery && !info.gate_initialized?
            if q = @applier.resolve_queue(info.vhost_id, info.queue_id)
              q.enable_commit_gate!(info.seg_id, pos.to_u32) unless q.raw_msg_store.commit_gated?
              info.gate_initialized = true
              @lock.synchronize { @segment_path_cache[path] = info }
            end
          end
          entry = @node.propose_publish(info.vhost_id, info.queue_id, info.seg_id, pos.to_u32, length.to_u32)
          @last_proposed_lsn.set(entry.lsn)
        when .consumer_offsets?
          return unless @node.leader?
          if File.exists?(path)
            copy = Bytes.new(length)
            File.open(path, "r") do |f|
              f.seek(pos)
              f.read_fully(copy)
            end
            entry = @node.propose_command(LogCommandType::StreamOffsetCommit, copy, vhost_id: info.vhost_id, queue_id: info.queue_id)
            @last_proposed_lsn.set(entry.lsn)
          end
        end
      rescue ex : RaftNode::Error::NotLeaderError
        if info && info.kind.msgs?
          abort_inflight_publish!(info, pos.to_u32)
          raise Clustering::Replicator::ReplicationError.new(ex.message || "Node #{@node.node_id} is not the cluster leader")
        end
      rescue IO::EOFError
      end

      def append_value(path : String, value : UInt32 | Int32, offset : Int64)
        return if replication_suppressed? || @server.nil? || !@node.leader?
        info = @lock.synchronize { @segment_path_cache[path]? } || cache_segment_path_info(path) || return
        if info.kind.acks?
          entry = @node.propose_ack(info.vhost_id, info.queue_id, info.seg_id, value.to_u32)
          @last_proposed_lsn.set(entry.lsn)
        end
      rescue RaftNode::Error::NotLeaderError
      end

      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        return if replication_suppressed? || @server.nil? || !@node.leader?
        rel = relative_path(path)
        parts = rel.split('/')

        if parts.size == 2 && parts[1] == "definitions.amqp"
          vhost_dir = parts[0]
          vhost_id = ResourceId.vhost_id_for_dir(vhost_dir)
          cmd_type, res_id = classify_definition_frame(vhost_dir, bytes)
          copy = Bytes.new(bytes.size)
          copy.copy_from(bytes)
          entry = @node.propose_command(cmd_type, copy, vhost_id: vhost_id, queue_id: res_id)
          @last_proposed_lsn.set(entry.lsn)
        elsif parts.size == 3 && parts[1] == "mqtt_retained_store" && parts[2] == "index"
          vhost_dir = parts[0]
          topic = String.new(bytes).chomp
          unless topic.empty?
            msg_file = "#{Digest::MD5.hexdigest(topic)}.msg"
            @lock.synchronize do
              @mqtt_file_to_topic["#{vhost_dir}/#{msg_file}"] = topic
            end
          end
        end
      rescue RaftNode::Error::NotLeaderError
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def delete_file(path : String)
        info = @lock.synchronize do
          @segment_path_cache.delete(path)
        end || parse_segment_path_info(path)

        if info && info.kind.msgs?
          # Stage any in-ring publish entries to raft.ctrl and release the MFile lease
          # before MessageStore#delete_file calls file.close
          @log.unregister_segment(info.vhost_id, info.queue_id, info.seg_id)
        end

        return if replication_suppressed? || @server.nil? || !@node.leader?

        if info && info.kind.msgs?
          # Only emit SegmentDelete when the queue itself still exists (not during queue deletion)
          queue_dot_file = File.join(File.dirname(path), ".queue")
          return unless File.exists?(queue_dot_file)
          entry = @node.propose_segment_delete(info.vhost_id, info.queue_id, info.seg_id)
          @last_proposed_lsn.set(entry.lsn)
          return
        end

        rel = relative_path(path)
        parts = rel.split('/')
        if parts.size == 3 && parts[1] == "mqtt_retained_store" && parts[2].ends_with?(".msg")
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          file_name = parts[2]
          topic_id = Digest::CRC32.checksum(file_name).to_u64
          entry = @node.propose_command(LogCommandType::MqttRetainDelete, file_name.to_slice, vhost_id: vhost_id, queue_id: topic_id)
          @last_proposed_lsn.set(entry.lsn)
        end
      rescue RaftNode::Error::NotLeaderError
      end

      def delete_dir(path : String)
        return if replication_suppressed? || @server.nil? || !@node.leader?
        rel = relative_path(path)
        parts = rel.split('/')
        if parts.size == 1 && !parts[0].empty?
          vhost_dir = parts[0]
          vhost_id = ResourceId.vhost_id_for_dir(vhost_dir)
          @applier.remove_vhost!(vhost_id)
          entry = @node.propose_command(LogCommandType::VHostDelete, vhost_dir.to_slice, vhost_id: vhost_id)
          @last_proposed_lsn.set(entry.lsn)
        end
      rescue RaftNode::Error::NotLeaderError
      end

      def followers : Array(Clustering::Follower)
        Array(Clustering::Follower).new
      end

      def syncing_followers : Array(Clustering::Follower)
        Array(Clustering::Follower).new
      end

      def all_followers : Array(Clustering::Follower)
        Array(Clustering::Follower).new
      end

      def isr_dirty? : Bool
        false
      end

      def flush_isr : Nil
      end

      def abort_uncommitted! : Nil
        @node.abort_uncommitted!
      end

      def wait_for_followers(timeout : Time::Span = @wait_timeout) : Nil
        return if replication_suppressed? || @server.nil?
        unless @node.leader?
          raise Clustering::Replicator::ReplicationError.new("Node #{@node.node_id} is not the cluster leader")
        end
        target = @last_proposed_lsn.get(:acquire)
        return if target.zero?
        unless @node.wait_for_commit(target, timeout)
          raise Clustering::Replicator::ReplicationError.new("Failed to commit LSN #{target} within #{timeout}")
        end
      end

      def request_fsync(paths : Enumerable(String)) : Nil
        @node.log.fsync if Config.instance.sync? && @node.log.sync?
      end

      def request_syncfs : Nil
        @node.log.fsync if Config.instance.sync? && @node.log.sync?
      end

      def close
        @node.close
      end

      def listen(server : TCPServer)
      end

      def clear
        @lock.synchronize do
          @segment_path_cache.clear
          @mqtt_file_to_topic.clear
        end
      end

      def password : String
        @node.secret
      end

      private def relative_path(path : String) : String
        Path[path].relative_to(@data_dir).to_s
      rescue
        path
      end

      private def cache_segment_path_info(path : String) : SegmentPathInfo?
        if info = parse_segment_path_info(path)
          @lock.synchronize do
            @segment_path_cache[path] = info
          end
          info
        end
      end

      private def parse_segment_path_info(path : String) : SegmentPathInfo?
        rel = relative_path(path)
        parts = rel.split('/')
        return unless parts.size == 3
        filename = parts[2]
        if filename.starts_with?("msgs.") && filename.size == 15
          seg_id = filename[5, 10].to_u32? || return
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          queue_id = ResourceId.queue_id_for_dirs(parts[0], parts[1])
          SegmentPathInfo.new(vhost_id, queue_id, seg_id, SegmentFileKind::Msgs)
        elsif filename.starts_with?("acks.") && filename.size == 15
          seg_id = filename[5, 10].to_u32? || return
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          queue_id = ResourceId.queue_id_for_dirs(parts[0], parts[1])
          SegmentPathInfo.new(vhost_id, queue_id, seg_id, SegmentFileKind::Acks)
        elsif filename == "consumer_offsets"
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          queue_id = ResourceId.queue_id_for_dirs(parts[0], parts[1])
          SegmentPathInfo.new(vhost_id, queue_id, 0_u32, SegmentFileKind::ConsumerOffsets)
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def classify_definition_frame(vhost_dir : String, bytes : Bytes) : Tuple(LogCommandType, UInt64)
        io = IO::Memory.new(bytes)
        stream = AMQ::Protocol::Stream.new(io, format: IO::ByteFormat::SystemEndian)
        frame = stream.next_frame
        case frame
        when AMQP::Frame::Queue::Declare
          q_id = ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.queue_name))
          if srv = @server
            vh_id = ResourceId.vhost_id_for_dir(vhost_dir)
            srv.vhosts.each_value do |vh|
              next unless vh.dir == vhost_dir
              if q = vh.queue?(frame.queue_name)
                q_id = @applier.index_queue!(vh, vh_id, q)
                q.enable_commit_gate! if @commit_gated_delivery && q.durable?
              end
              break
            end
          end
          {LogCommandType::QueueDeclare, q_id}
        when AMQP::Frame::Queue::Delete
          vh_id = ResourceId.vhost_id_for_dir(vhost_dir)
          q_id = ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.queue_name))
          @applier.remove_queue!(vh_id, q_id)
          {LogCommandType::QueueDelete, q_id}
        when AMQP::Frame::Exchange::Declare
          {LogCommandType::ExchangeDeclare, ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.exchange_name))}
        when AMQP::Frame::Exchange::Delete
          {LogCommandType::ExchangeDelete, ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.exchange_name))}
        when AMQP::Frame::Queue::Bind
          {LogCommandType::BindingAdd, ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.queue_name))}
        when AMQP::Frame::Exchange::Bind
          {LogCommandType::BindingAdd, ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.destination))}
        when AMQP::Frame::Queue::Unbind
          {LogCommandType::BindingRemove, ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.queue_name))}
        when AMQP::Frame::Exchange::Unbind
          {LogCommandType::BindingRemove, ResourceId.queue_id_for_dirs(vhost_dir, Digest::SHA1.hexdigest(frame.destination))}
        else
          {LogCommandType::QueueDeclare, 0_u64}
        end
      rescue
        {LogCommandType::QueueDeclare, 0_u64}
      end

      private def resolve_mqtt_topic(vhost_dir : String, msg_file : String) : String?
        if topic = @lock.synchronize { @mqtt_file_to_topic["#{vhost_dir}/#{msg_file}"]? }
          return topic
        end
        index_path = File.join(@data_dir, vhost_dir, "mqtt_retained_store", "index")
        return unless File.exists?(index_path)
        File.each_line(index_path) do |line|
          t = line.chomp
          next if t.empty?
          if "#{Digest::MD5.hexdigest(t)}.msg" == msg_file
            @lock.synchronize { @mqtt_file_to_topic["#{vhost_dir}/#{msg_file}"] = t }
            return t
          end
        end
        nil
      end
    end
  end
end
