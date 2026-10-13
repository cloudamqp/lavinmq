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

      @data_dir : String
      @mfiles = Hash(String, MFile).new
      @mqtt_file_to_topic = Hash(String, String).new
      @last_proposed_lsn = Atomic(UInt64).new(0_u64)
      @lock = Mutex.new(:reentrant)
      @on_role_change : Proc(Role, Nil)?

      def initialize(@data_dir : String,
                     @node : RaftNode,
                     @commit_gated_delivery : Bool = true)
        @log = @node.log
        @applier = Applier.new(@log) do |suppress|
          @suppressed = suppress
        end

        @log.on_truncate do |entry|
          @applier.rollback_uncommitted(entry)
        end

        @node.on_apply do |rec|
          @applier.apply_committed(rec, from_leader: @node.leader?)
        end

        @node.on_promote do |_term|
          handle_promotion
        end

        @node.on_step_down do |_term|
          handle_step_down
        end

        @node.on_build_snapshot do |_snap_lsn|
          build_snapshot_segments
        end

        @node.on_install_snapshot do |req|
          @applier.install_snapshot(req)
        end
      end

      def on_role_change(&block : Role -> Nil) : Nil
        @on_role_change = block
      end

      def attach(server : LavinMQ::Server) : Nil
        @server = server
        @applier.server = server
        @applier.register_all_segments!
        if @node.leader? && @commit_gated_delivery
          enable_commit_gates!
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
        @on_role_change.try &.call(Role::Leader)
      end

      # Clean in-process step-down without terminating the process (`exit 3` is eliminated).
      private def handle_step_down : Nil
        if srv = @server
          srv.vhosts.each_value do |vh|
            vh.connections.dup.each do |conn|
              conn.close("Stepping down from cluster leader") rescue nil
            end
          end
        end
        disable_commit_gates!
        @on_role_change.try &.call(@node.role)
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def build_snapshot_segments : Array(SnapshotSegmentFile)
        result = Array(SnapshotSegmentFile).new
        srv = @server || return result

        srv.vhosts.each_value do |vh|
          vh.queues.each do |q|
            next unless q.durable?
            q_dir = File.join(vh.data_dir, Digest::SHA1.hexdigest(q.name))
            next unless Dir.exists?(q_dir)

            Dir.each_child(q_dir) do |child|
              next unless child.starts_with?("msgs.") || child.starts_with?("acks.") || child == "consumer_offsets"
              full_path = File.join(q_dir, child)
              next unless File.file?(full_path)
              rel = Path[full_path].relative_to(@data_dir).to_s
              seg_id = (child.starts_with?("msgs.") || child.starts_with?("acks.")) && child.size == 15 ? (child[5, 10].to_u32? || 0_u32) : 0_u32
              if mfile = @mfiles[full_path]?
                slice = mfile.to_slice(0, mfile.size)
                copy = Bytes.new(slice.size)
                copy.copy_from(slice)
                result << SnapshotSegmentFile.new(vh.name, q.name, seg_id, rel, copy)
              else
                result << SnapshotSegmentFile.new(vh.name, q.name, seg_id, rel, File.read(full_path).to_slice)
              end
            end
          end
        end
        result
      end

      # --- LavinMQ::Clustering::Replicator interface ---

      def register_file(path : String)
      end

      def register_file(file : File)
      end

      def register_file(mfile : MFile)
        @lock.synchronize do
          @mfiles[mfile.path] = mfile
        end
        if ids = parse_queue_segment_path(mfile.path)
          vhost_id, queue_id, prefix, seg_id = ids
          if prefix == "msgs"
            @log.register_segment(vhost_id, queue_id, seg_id, mfile)
          end
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def replace_file(path : String)
        return if @suppressed || @server.nil? || !@node.leader?
        rel = relative_path(path)
        parts = rel.split('/')

        if parts.size == 1 && parts[0] == "vhosts.json"
          return unless File.exists?(path)
          payload = File.read(path).to_slice
          entry = @node.propose_command(LogCommandType::VHostCreate, payload)
          @last_proposed_lsn.set(entry.lsn)
        elsif parts.size == 3 && parts[1] == "mqtt_retained_store" && parts[2].ends_with?(".msg")
          return unless File.exists?(path)
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

          entry = @node.propose_command(LogCommandType::MqttRetainPut, io.to_slice, vhost_id: vhost_id)
          @last_proposed_lsn.set(entry.lsn)
        end
      rescue RaftNode::Error::NotLeaderError
      end

      def replace_file(mfile : MFile)
        @lock.synchronize do
          @mfiles[mfile.path] = mfile
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def append(path : String, pos : Int, length : Int)
        return if @suppressed || @server.nil? || !@node.leader?
        rel = relative_path(path)
        parts = rel.split('/')
        return unless parts.size == 3

        vhost_dir = parts[0]
        queue_dir = parts[1]
        filename = parts[2]
        vhost_id = ResourceId.vhost_id_for_dir(vhost_dir)
        queue_id = ResourceId.queue_id_for_dirs(vhost_dir, queue_dir)

        if filename.starts_with?("msgs.") && filename.size == 15
          seg_id = filename[5, 10].to_u32
          if @commit_gated_delivery
            if q = @applier.resolve_queue(vhost_id, queue_id)
              q.enable_commit_gate! unless q.raw_msg_store.commit_gated?
            end
          end
          if mfile = @lock.synchronize { @mfiles[path]? }
            @log.register_segment(vhost_id, queue_id, seg_id, mfile)
          end
          entry = @node.propose_publish(vhost_id, queue_id, seg_id, pos.to_u32, length.to_u32)
          @last_proposed_lsn.set(entry.lsn)
        elsif filename == "consumer_offsets"
          if mfile = @lock.synchronize { @mfiles[path]? }
            slice = mfile.to_slice(pos, length)
            copy = Bytes.new(slice.size)
            copy.copy_from(slice)
            entry = @node.propose_command(LogCommandType::StreamOffsetCommit, copy, vhost_id: vhost_id, queue_id: queue_id)
            @last_proposed_lsn.set(entry.lsn)
          end
        end
      rescue RaftNode::Error::NotLeaderError
      end

      def append_value(path : String, value : UInt32 | Int32, offset : Int64)
        return if @suppressed || @server.nil? || !@node.leader?
        if ids = parse_queue_segment_path(path)
          vhost_id, queue_id, prefix, seg_id = ids
          if prefix == "acks"
            entry = @node.propose_ack(vhost_id, queue_id, seg_id, value.to_u32)
            @last_proposed_lsn.set(entry.lsn)
          end
        end
      rescue RaftNode::Error::NotLeaderError
      end

      # ameba:disable Metrics/CyclomaticComplexity
      def append_bytes(path : String, bytes : Bytes, offset : Int64)
        return if @suppressed || @server.nil? || !@node.leader?
        rel = relative_path(path)
        parts = rel.split('/')

        if parts.size == 2 && parts[1] == "definitions.amqp"
          vhost_dir = parts[0]
          vhost_id = ResourceId.vhost_id_for_dir(vhost_dir)
          cmd_type = classify_definition_frame(bytes)
          copy = Bytes.new(bytes.size)
          copy.copy_from(bytes)
          if cmd_type == LogCommandType::QueueDeclare && @commit_gated_delivery
            enable_commit_gates!
          end
          entry = @node.propose_command(cmd_type, copy, vhost_id: vhost_id)
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

      def delete_file(path : String)
        @lock.synchronize do
          @mfiles.delete(path)
        end
        return if @suppressed || @server.nil? || !@node.leader?

        rel = relative_path(path)
        parts = rel.split('/')
        if parts.size == 3 && parts[2].starts_with?("msgs.") && parts[2].size == 15
          # Only emit SegmentDelete when the queue itself still exists (not during queue deletion)
          queue_dot_file = File.join(File.dirname(path), ".queue")
          return unless File.exists?(queue_dot_file)
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          queue_id = ResourceId.queue_id_for_dirs(parts[0], parts[1])
          seg_id = parts[2][5, 10].to_u32
          entry = @node.propose_segment_delete(vhost_id, queue_id, seg_id)
          @last_proposed_lsn.set(entry.lsn)
        elsif parts.size == 3 && parts[1] == "mqtt_retained_store" && parts[2].ends_with?(".msg")
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          file_name = parts[2]
          entry = @node.propose_command(LogCommandType::MqttRetainDelete, file_name.to_slice, vhost_id: vhost_id)
          @last_proposed_lsn.set(entry.lsn)
        end
      rescue RaftNode::Error::NotLeaderError
      end

      def delete_dir(path : String)
        return if @suppressed || @server.nil? || !@node.leader?
        rel = relative_path(path)
        parts = rel.split('/')
        if parts.size == 1 && !parts[0].empty?
          vhost_dir = parts[0]
          entry = @node.propose_command(LogCommandType::VHostDelete, vhost_dir.to_slice)
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

      def wait_for_followers : Nil
        return if @suppressed || @server.nil? || !@node.leader?
        target = @last_proposed_lsn.get(:acquire)
        return if target.zero?
        @node.wait_for_commit(target, @wait_timeout)
      end

      def request_fsync(paths : Enumerable(String)) : Nil
      end

      def request_syncfs : Nil
      end

      def close
        @node.close
      end

      def listen(server : TCPServer)
      end

      def clear
        @lock.synchronize do
          @mfiles.clear
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

      private def parse_queue_segment_path(path : String) : Tuple(UInt16, UInt32, String, UInt32)?
        rel = relative_path(path)
        parts = rel.split('/')
        return unless parts.size == 3
        filename = parts[2]
        if (filename.starts_with?("msgs.") || filename.starts_with?("acks.")) && filename.size == 15
          prefix = filename[0, 4]
          seg_id = filename[5, 10].to_u32? || return
          vhost_id = ResourceId.vhost_id_for_dir(parts[0])
          queue_id = ResourceId.queue_id_for_dirs(parts[0], parts[1])
          return {vhost_id, queue_id, prefix, seg_id}
        end
        nil
      end

      private def classify_definition_frame(bytes : Bytes) : LogCommandType
        io = IO::Memory.new(bytes)
        stream = AMQ::Protocol::Stream.new(io, format: IO::ByteFormat::SystemEndian)
        frame = stream.next_frame
        case frame
        when AMQP::Frame::Queue::Declare
          LogCommandType::QueueDeclare
        when AMQP::Frame::Queue::Delete
          LogCommandType::QueueDelete
        when AMQP::Frame::Exchange::Declare
          LogCommandType::ExchangeDeclare
        when AMQP::Frame::Exchange::Delete
          LogCommandType::ExchangeDelete
        when AMQP::Frame::Queue::Bind, AMQP::Frame::Exchange::Bind
          LogCommandType::BindingAdd
        when AMQP::Frame::Queue::Unbind, AMQP::Frame::Exchange::Unbind
          LogCommandType::BindingRemove
        else
          LogCommandType::QueueDeclare
        end
      rescue
        LogCommandType::QueueDeclare
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
