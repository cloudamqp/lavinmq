require "socket"
require "../../stdlib/channel"
require "../logger"
require "./protocol"
require "./raft_log"
require "./peer_session"

module LavinMQ
  module ClusteringV2
    class RaftNode
      Log = LavinMQ::Log.for "clustering_v2.raft"

      alias ApplyCallback = LogEntryRecord -> Nil
      alias TruncateCallback = LogIndexEntry -> Nil
      alias PromoteCallback = UInt64 -> Nil
      alias StepDownCallback = UInt64 -> Nil
      alias SnapshotBuilderCallback = UInt64 -> Array(SnapshotSegmentFile)
      alias SnapshotInstallerCallback = InstallSnapshotRequest -> Nil

      private struct CommitWaiter
        getter target_lsn : UInt64
        getter channel : ::Channel(Bool)

        def initialize(@target_lsn : UInt64, @channel : ::Channel(Bool))
        end
      end

      getter log : RaftLog
      getter node_id : UInt64
      getter secret : String
      getter execution_context : Fiber::ExecutionContext
      getter role : Role = Role::Follower
      getter leader_id : UInt64 = 0_u64
      getter last_applied : UInt64 = 0_u64
      property peer_uri : String
      property amqp_uri : String
      property http_uri : String
      property heartbeat_interval : Time::Span = 50.milliseconds
      getter election_timeout_min : Time::Span = 200.milliseconds
      getter election_timeout_max : Time::Span = 400.milliseconds
      property leader_lease_timeout : Time::Span = 350.milliseconds
      property leader_stickiness_window : Time::Span = 150.milliseconds
      property learner_promote_threshold : UInt64 = 32_u64
      property? auto_promote_learners : Bool = true

      def election_timeout_min=(@election_timeout_min : Time::Span) : Time::Span
        if @election_timeout_max >= @election_timeout_min
          @current_election_timeout = random_election_timeout
        end
        @election_timeout_min
      end

      def election_timeout_max=(@election_timeout_max : Time::Span) : Time::Span
        if @election_timeout_max >= @election_timeout_min
          @current_election_timeout = random_election_timeout
        end
        @election_timeout_max
      end

      @lock = Mutex.new(:reentrant)
      @apply_lock = Mutex.new(:reentrant)
      @peers = Hash(UInt64, PeerSession).new
      @peer_notify = Hash(UInt64, ::Channel(Bool)).new
      @peer_force_send = Hash(UInt64, Bool).new
      @peer_in_flight_until = Hash(UInt64, Time::Instant).new
      @peer_last_sent_commit = Hash(UInt64, UInt64).new
      @snapshot_in_flight = Hash(UInt64, Time::Instant).new
      @apply_notify = ::Channel(Bool).new(1)
      @next_index = Hash(UInt64, UInt64).new
      @match_index = Hash(UInt64, UInt64).new
      @last_ack_at = Hash(UInt64, Time::Instant).new
      @pre_votes_granted = Set(UInt64).new
      @votes_granted = Set(UInt64).new
      @commit_waiters = Array(CommitWaiter).new
      @pending_truncations = Deque(LogIndexEntry).new
      @quorum_scratch = Array(UInt64).new(16)
      @control_history = Array(LogEntryRecord).new
      @ctrl_hist_path : String
      @last_leader_contact : Time::Instant = Time.instant
      @last_heartbeat_sent : Time::Instant = Time.instant
      @current_election_timeout : Time::Span = 300.milliseconds
      @promoted_term : UInt64 = 0_u64
      @pending_conf_change_lsn : UInt64 = 0_u64
      @listener : TCPServer?
      @cluster_configured : Bool = false
      @started = false
      @closed = false
      @isolated = false

      @on_apply : ApplyCallback?
      @on_truncate : TruncateCallback?
      @on_promote : PromoteCallback?
      @on_step_down : StepDownCallback?
      @snapshot_builder : SnapshotBuilderCallback?
      @snapshot_installer : SnapshotInstallerCallback?

      CTRL_HIST_MAGIC = 0x4C4D5148_u32

      def initialize(@log : RaftLog,
                     @secret : String,
                     @peer_uri : String = "",
                     @amqp_uri : String = "",
                     @http_uri : String = "",
                     initial_role : MemberRole = MemberRole::Voter,
                     execution_context : Fiber::ExecutionContext? = nil)
        raise ArgumentError.new("Cluster secret must not be empty") if @secret.blank?
        @node_id = @log.node_id
        @execution_context = execution_context || Fiber::ExecutionContext::Parallel.new("ClusteringV2-Raft-#{@node_id}", 4)
        @role = initial_role.learner? ? Role::Learner : Role::Follower
        @current_election_timeout = random_election_timeout
        @log.track_applied = true
        @last_applied = @log.applied_index
        @ctrl_hist_path = File.join(@log.data_dir, "raft.ctrl_hist")
        load_control_history

        @log.on_truncate do |entry|
          @lock.synchronize { @pending_truncations << entry }
          signal_apply
        end

        @log.on_membership_change do |members|
          @lock.synchronize { on_log_membership_changed_unsafe(members) }
        end

        if @log.meta_existed? && !@log.members.empty?
          @cluster_configured = true
          if me = @log.members.find { |m| m.node_id == @node_id }
            @role = me.learner? ? Role::Learner : Role::Follower
          end
        elsif !@peer_uri.empty?
          @cluster_configured = true
          if @log.members.empty?
            @log.set_members([MemberInfo.new(@node_id, @peer_uri, @amqp_uri, @http_uri, initial_role)])
          end
        end

        @execution_context.spawn(name: "ClusteringV2 applier #{@node_id}") { apply_loop }
      end

      def on_apply(&block : LogEntryRecord -> Nil) : Nil
        @on_apply = block
      end

      def on_truncate(&block : LogIndexEntry -> Nil) : Nil
        @on_truncate = block
      end

      def on_promote(&block : UInt64 -> Nil) : Nil
        @on_promote = block
      end

      def on_step_down(&block : UInt64 -> Nil) : Nil
        @on_step_down = block
      end

      def on_build_snapshot(&block : UInt64 -> Array(SnapshotSegmentFile)) : Nil
        @snapshot_builder = block
      end

      def on_install_snapshot(&block : InstallSnapshotRequest -> Nil) : Nil
        @snapshot_installer = block
      end

      def leader? : Bool
        @lock.synchronize { @role.leader? }
      end

      def follower? : Bool
        @lock.synchronize { @role.follower? || @role.learner? }
      end

      def current_term : UInt64
        @log.current_term
      end

      def commit_index : UInt64
        @log.commit_index
      end

      def leader_member : MemberInfo?
        @lock.synchronize do
          return if @leader_id == 0
          @log.members.find { |m| m.node_id == @leader_id }
        end
      end

      def members : Array(MemberInfo)
        @log.members
      end

      def match_index_for(peer_id : UInt64) : UInt64
        @lock.synchronize do
          return @log.last_lsn if peer_id == @node_id
          @match_index[peer_id]? || 0_u64
        end
      end

      def start(bind_host : String = "127.0.0.1", bind_port : Int32 = 0) : Nil
        server = TCPServer.new(bind_host, bind_port)
        @listener = server
        addr = server.local_address
        @peer_uri = "tcp://#{addr.address}:#{addr.port}" if @peer_uri.empty?

        @lock.synchronize do
          @started = true
          if @cluster_configured
            my_role = @role.learner? ? MemberRole::Learner : MemberRole::Voter
            if existing = @log.members.find { |m| m.node_id == @node_id }
              my_role = existing.role
            end
            @log.upsert_member(MemberInfo.new(@node_id, @peer_uri, @amqp_uri, @http_uri, my_role))
          end
          sync_peer_sessions_unsafe
          @current_election_timeout = random_election_timeout
          @last_leader_contact = Time.instant
          @last_heartbeat_sent = Time.instant
        end

        @execution_context.spawn(name: "ClusteringV2 listener #{@node_id}") { accept_loop(server) }
        @execution_context.spawn(name: "ClusteringV2 ticker #{@node_id}") { tick_loop }
        signal_apply if @lock.synchronize { @last_applied < @log.commit_index }
      end

      def configure_cluster(members : Array(MemberInfo)) : Nil
        @lock.synchronize do
          @cluster_configured = true
          @log.set_members(members)
          if me = members.find { |m| m.node_id == @node_id }
            if me.learner? && !@role.leader?
              @role = Role::Learner
            elsif me.voter? && @role.learner?
              @role = Role::Follower
            end
          end
          sync_peer_sessions_unsafe
        end
      end

      # Proposes a new membership change (AddLearner, PromoteLearner, RemoveNode) on the leader.
      def propose_conf_change(change_type : ConfChangeType, member : MemberInfo) : LogIndexEntry
        cmd = ConfChangeCommand.new(change_type, member)
        propose_command(LogCommandType::ConfChange, cmd.to_slice)
      end

      def propose_publish(vhost_id : UInt16,
                          queue_id : UInt32,
                          segment_id : UInt32,
                          position : UInt32,
                          byte_size : UInt32,
                          fallback_payload : Bytes? = nil) : LogIndexEntry
        committed_now = false
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_publish(@log.current_term, vhost_id, queue_id, segment_id, position, byte_size, fallback_payload)
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          committed_now = @last_applied < @log.commit_index
          signal_all_peers_unsafe(force: false)
          e
        end
        signal_apply if committed_now
        entry
      end

      def propose_ack(vhost_id : UInt16,
                      queue_id : UInt32,
                      segment_id : UInt32,
                      position : UInt32) : LogIndexEntry
        committed_now = false
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_ack(@log.current_term, vhost_id, queue_id, segment_id, position)
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          committed_now = @last_applied < @log.commit_index
          signal_all_peers_unsafe(force: false)
          e
        end
        signal_apply if committed_now
        entry
      end

      def propose_segment_delete(vhost_id : UInt16,
                                 queue_id : UInt32,
                                 segment_id : UInt32) : LogIndexEntry
        committed_now = false
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_segment_delete(@log.current_term, vhost_id, queue_id, segment_id)
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          committed_now = @last_applied < @log.commit_index
          signal_all_peers_unsafe(force: false)
          e
        end
        signal_apply if committed_now
        entry
      end

      def propose_command(cmd_type : LogCommandType,
                          payload : Bytes = Bytes.empty,
                          vhost_id : UInt16 = 0_u16,
                          queue_id : UInt32 = 0_u32,
                          segment_id : UInt32 = 0_u32,
                          position : UInt32 = 0_u32) : LogIndexEntry
        committed_now = false
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_command(@log.current_term, cmd_type, payload, vhost_id, queue_id, segment_id, position)
          if cmd_type.conf_change?
            @pending_conf_change_lsn = e.lsn
          end
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          committed_now = @last_applied < @log.commit_index
          signal_all_peers_unsafe(force: false)
          e
        end
        signal_apply if committed_now
        entry
      end

      # Blocks the caller until `target_lsn <= commit_index` and applied, or returns false if
      # the node steps down / times out before committing `target_lsn`.
      def wait_for_commit(target_lsn : UInt64, timeout : Time::Span? = nil) : Bool
        ch : ::Channel(Bool)? = nil
        needs_apply = false

        @lock.synchronize do
          if @log.commit_index >= target_lsn && @last_applied >= target_lsn
            return true
          end
          return false unless @role.leader?
          c = ::Channel(Bool).new(1)
          @commit_waiters << CommitWaiter.new(target_lsn, c)
          ch = c
          needs_apply = @log.commit_index >= target_lsn
          signal_all_peers_unsafe(force: false)
        end

        signal_apply if needs_apply
        wait_ch = ch || return false

        if t = timeout
          select
          when res = wait_ch.receive?
            res || false
          when timeout(t)
            @lock.synchronize { @commit_waiters.reject! { |w| w.channel == wait_ch } }
            false
          end
        else
          wait_ch.receive? || false
        end
      end

      # Isolates or reconnects this node from all peers (for partition / chaos testing).
      # ameba:disable Naming/AccessorMethodName
      def set_isolated(isolated : Bool) : Nil
        @lock.synchronize do
          @isolated = isolated
          @peers.each_value do |p|
            p.paused = isolated
            p.disconnect! if isolated
          end
          unless isolated
            @peer_in_flight_until.clear
            @snapshot_in_flight.clear
            signal_all_peers_unsafe(force: true) if @role.leader?
          end
        end
      end

      def pause_peer(peer_id : UInt64, paused : Bool = true) : Nil
        @lock.synchronize do
          if peer = @peers[peer_id]?
            peer.paused = paused
          end
          unless paused
            @peer_in_flight_until.delete(peer_id)
            @snapshot_in_flight.delete(peer_id)
            signal_peer_unsafe(peer_id, force: true) if @role.leader?
          end
        end
      end

      def trigger_election! : Nil
        start_pre_vote
      end

      # Aborts any uncommitted log entries by stepping down (if leader) and truncating
      # entries above `commit_index` so timed-out proposals cannot commit later.
      def abort_uncommitted! : Nil
        stepped_down = false
        term = 0_u64
        @lock.synchronize do
          return if @closed
          term = @log.current_term
          if @role.leader?
            stepped_down = step_down_unsafe(term)
          elsif @log.last_lsn > @log.commit_index
            @log.truncate_from(@log.commit_index + 1)
          end
        end
        drain_pending_truncations
        @on_step_down.try &.call(term) if stepped_down
      end

      def close : Nil
        @lock.synchronize do
          return if @closed
          @closed = true
          @listener.try &.close rescue nil
          @peer_notify.each_value(&.close)
          @peer_notify.clear
          @apply_notify.close
          @peers.each_value(&.close)
          @peers.clear
          @commit_waiters.each(&.channel.try_send(false))
          @commit_waiters.clear
          @log.close
        end
      end

      private def ensure_leader_unsafe! : Nil
        raise Error::NotLeaderError.new(@leader_id) unless @role.leader?
      end

      private def voter_unsafe?(node_id : UInt64) : Bool
        @log.members.any? { |m| m.node_id == node_id && m.voter? }
      end

      private def accept_loop(server : TCPServer) : Nil
        loop do
          break if @closed
          sock = server.accept? || break
          sock.tcp_nodelay = true
          sock.sync = false
          @execution_context.spawn(name: "ClusteringV2 inbound handshake") do
            remote_id, remote_uri = Protocol.server_handshake!(sock, @node_id, @secret)
            if @isolated
              sock.close rescue nil
              next
            end
            session = @lock.synchronize do
              s = @peers[remote_id]?
              if s
                s.peer_uri = remote_uri unless remote_uri.empty?
              else
                s = create_peer_session_unsafe(remote_id, remote_uri)
              end
              s
            end
            session.attach_inbound_socket(sock)
          rescue
            sock.close rescue nil
          end
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def tick_loop : Nil
        loop do
          sleep 10.milliseconds
          break if @closed

          should_replicate = false
          should_pre_vote = false
          won_single_node = false
          stepped_down_term : UInt64? = nil

          @lock.synchronize do
            return if @closed
            now = Time.instant

            case @role
            when .leader?
              if check_leader_lease_expired_unsafe(now)
                stepped_down_term = @log.current_term
                step_down_unsafe(@log.current_term)
              elsif now - @last_heartbeat_sent >= @heartbeat_interval
                @last_heartbeat_sent = now
                should_replicate = true
              end
            when .follower?, .pre_candidate?, .candidate?
              if @cluster_configured && voter_unsafe?(@node_id)
                if voting_single_node_cluster_unsafe?
                  become_candidate_and_win_single_node_unsafe
                  won_single_node = true
                elsif now - @last_leader_contact >= @current_election_timeout
                  should_pre_vote = true
                end
              end
            when .learner?
              # Learners do not initiate elections
            end
          end

          if term = stepped_down_term
            drain_pending_truncations
            @on_step_down.try &.call(term)
          elsif won_single_node
            signal_apply
          elsif should_replicate
            replicate_to_peers(force: true)
            signal_apply
          elsif should_pre_vote
            start_pre_vote
          end
        end
      end

      private def voting_single_node_cluster_unsafe? : Bool
        return false unless @cluster_configured
        voter_count = 0
        only_voter_id = 0_u64
        @log.members.each do |m|
          next unless m.voter?
          voter_count += 1
          only_voter_id = m.node_id
          return false if voter_count > 1
        end
        voter_count == 1 && only_voter_id == @node_id
      end

      private def become_candidate_and_win_single_node_unsafe : Nil
        new_term = @log.current_term + 1
        @log.set_term_and_vote(new_term, @node_id)
        become_leader_unsafe
      end

      private def check_leader_lease_expired_unsafe(now : Time::Instant) : Bool
        voter_count = 0
        active = 0
        @log.members.each do |m|
          next unless m.voter?
          voter_count += 1
          if m.node_id == @node_id
            active += 1
          elsif last_ack = @last_ack_at[m.node_id]?
            active += 1 if (now - last_ack) <= @leader_lease_timeout
          end
        end
        return false if voter_count <= 1
        quorum = (voter_count // 2) + 1
        active < quorum
      end

      private def start_pre_vote : Nil
        req : VoteRequest? = nil
        peers_to_send = Array(PeerSession).new
        won_single_node = false

        @lock.synchronize do
          return if @closed || @role.leader? || @role.learner? || @isolated
          return unless @cluster_configured && voter_unsafe?(@node_id)
          if voting_single_node_cluster_unsafe?
            become_candidate_and_win_single_node_unsafe
            won_single_node = true
          else
            @role = Role::PreCandidate
            @last_leader_contact = Time.instant
            @current_election_timeout = random_election_timeout
            @pre_votes_granted.clear
            @pre_votes_granted.add(@node_id)

            next_term = @log.current_term + 1
            req = VoteRequest.new(
              pre_vote: true,
              term: next_term,
              candidate_id: @node_id,
              last_log_lsn: @log.last_lsn,
              last_log_term: @log.last_log_term
            )
            sync_peer_sessions_unsafe
            peers_to_send = @peers.values.select { |p| voter_unsafe?(p.peer_id) }
          end
        end

        signal_apply
        return if won_single_node

        if vote_req = req
          peers_to_send.each do |peer|
            @execution_context.spawn(name: "ClusteringV2 pre-vote #{@node_id}->#{peer.peer_id}") do
              peer.send_vote_request(vote_req)
            end
          end
        end
      end

      private def start_real_election_unsafe : Tuple(VoteRequest?, Array(PeerSession))
        return {nil, Array(PeerSession).new} unless @cluster_configured && voter_unsafe?(@node_id)
        new_term = @log.current_term + 1
        @log.set_term_and_vote(new_term, @node_id)
        @role = Role::Candidate
        @leader_id = 0_u64
        @last_leader_contact = Time.instant
        @current_election_timeout = random_election_timeout
        @votes_granted.clear
        @votes_granted.add(@node_id)

        if @votes_granted.size >= quorum_size_unsafe
          become_leader_unsafe
          return {nil, Array(PeerSession).new}
        end

        req = VoteRequest.new(
          pre_vote: false,
          term: new_term,
          candidate_id: @node_id,
          last_log_lsn: @log.last_lsn,
          last_log_term: @log.last_log_term
        )
        {req, @peers.values.select { |p| voter_unsafe?(p.peer_id) }}
      end

      private def become_leader_unsafe : Nil
        @role = Role::Leader
        @leader_id = @node_id
        now = Time.instant
        @last_heartbeat_sent = now
        @peer_in_flight_until.clear
        @peer_last_sent_commit.clear
        @snapshot_in_flight.clear

        next_lsn = @log.last_lsn + 1
        @log.members.each do |m|
          if m.node_id == @node_id
            @match_index[m.node_id] = @log.last_lsn
            @next_index[m.node_id] = next_lsn
          else
            @match_index[m.node_id] = 0_u64
            @next_index[m.node_id] = next_lsn
            @last_ack_at[m.node_id] = now
          end
        end

        noop = @log.append_command(@log.current_term, LogCommandType::LeaderNoOp)
        @match_index[@node_id] = noop.lsn
        try_advance_commit_index_unsafe
        signal_all_peers_unsafe(force: true)
      end

      private def step_down_unsafe(new_term : UInt64, new_leader_id : UInt64 = 0_u64) : Bool
        was_leader = @role.leader?
        if new_term > @log.current_term
          @log.set_term_and_vote(new_term, 0_u64)
        end

        me = @log.members.find { |m| m.node_id == @node_id }
        @role = (me && me.learner?) ? Role::Learner : Role::Follower
        @leader_id = new_leader_id
        @pre_votes_granted.clear
        @votes_granted.clear
        @peer_in_flight_until.clear
        @snapshot_in_flight.clear

        if was_leader
          if @log.last_lsn > @log.commit_index
            @log.truncate_from(@log.commit_index + 1)
          end
          @commit_waiters.each(&.channel.try_send(false))
          @commit_waiters.clear
        end
        was_leader
      end

      private def handle_peer_frame(peer_id : UInt64, frame_type : FrameType, io : IO) : Nil
        case frame_type
        in .request_pre_vote?, .request_vote?
          req = VoteRequest.from_io(io)
          handle_vote_request(peer_id, req)
        in .request_pre_vote_response?, .request_vote_response?
          resp = VoteResponse.from_io(io)
          handle_vote_response(peer_id, resp)
        in .append_entries?
          req = AppendEntriesRequest.from_io(io)
          handle_append_entries(peer_id, req)
        in .append_entries_response?
          resp = AppendEntriesResponse.from_io(io)
          handle_append_entries_response(peer_id, resp)
        in .install_snapshot?
          req = InstallSnapshotRequest.from_io(io)
          handle_install_snapshot(peer_id, req)
        in .install_snapshot_response?
          resp = InstallSnapshotResponse.from_io(io)
          handle_install_snapshot_response(peer_id, resp)
        in .handshake_hello?, .handshake_challenge?, .handshake_response?, .handshake_ack?
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def handle_vote_request(peer_id : UInt64, req : VoteRequest) : Nil
        resp : VoteResponse? = nil
        stepped_down = false
        stepped_down_term = 0_u64
        peer : PeerSession? = nil

        @lock.synchronize do
          return if @closed || @isolated
          peer = @peers[peer_id]?
          now = Time.instant
          both_voters = voter_unsafe?(@node_id) && voter_unsafe?(req.candidate_id) && req.candidate_id == peer_id
          leader_recently_active = @leader_id != 0 && @leader_id != req.candidate_id &&
                                   (now - @last_leader_contact) < @leader_stickiness_window

          if req.pre_vote?
            grant = both_voters &&
                    !leader_recently_active &&
                    req.term >= @log.current_term &&
                    @log.up_to_date?(req.last_log_lsn, req.last_log_term)
            resp = VoteResponse.new(true, @log.current_term, @node_id, grant)
          else
            if both_voters && req.term > @log.current_term && !leader_recently_active
              stepped_down = step_down_unsafe(req.term)
              stepped_down_term = req.term
            end

            can_vote = (@log.voted_for == 0 || @log.voted_for == req.candidate_id)
            grant = both_voters &&
                    !leader_recently_active &&
                    req.term == @log.current_term &&
                    can_vote &&
                    @log.up_to_date?(req.last_log_lsn, req.last_log_term)

            if grant
              @log.set_term_and_vote(@log.current_term, req.candidate_id)
              @last_leader_contact = now
            end
            resp = VoteResponse.new(false, @log.current_term, @node_id, grant)
          end
        end

        if stepped_down
          drain_pending_truncations
          @on_step_down.try &.call(stepped_down_term)
        end
        if (r = resp) && (p = peer)
          p.send_vote_response(r)
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def handle_vote_response(peer_id : UInt64, resp : VoteResponse) : Nil
        real_vote_req : VoteRequest? = nil
        peers_to_send = Array(PeerSession).new
        became_leader = false
        stepped_down = false
        stepped_down_term = 0_u64

        @lock.synchronize do
          return if @closed || @isolated
          if resp.term > @log.current_term
            stepped_down = step_down_unsafe(resp.term)
            stepped_down_term = resp.term
            return
          end

          return unless resp.voter_id == peer_id && voter_unsafe?(resp.voter_id)

          if resp.pre_vote? && @role.pre_candidate? && resp.vote_granted?
            @pre_votes_granted.add(resp.voter_id)
            if @pre_votes_granted.size >= quorum_size_unsafe
              real_vote_req, peers_to_send = start_real_election_unsafe
              became_leader = @role.leader?
            end
          elsif !resp.pre_vote? && @role.candidate? && resp.term == @log.current_term && resp.vote_granted?
            @votes_granted.add(resp.voter_id)
            if @votes_granted.size >= quorum_size_unsafe
              become_leader_unsafe
              became_leader = true
            end
          end
        end

        if stepped_down
          drain_pending_truncations
          @on_step_down.try &.call(stepped_down_term)
        end
        if req = real_vote_req
          peers_to_send.each do |peer|
            @execution_context.spawn(name: "ClusteringV2 vote #{@node_id}->#{peer.peer_id}") do
              peer.send_vote_request(req)
            end
          end
        end
        if became_leader
          signal_apply
        end
      end

      def handle_append_entries(peer_id : UInt64, req : AppendEntriesRequest) : AppendEntriesResponse?
        resp : AppendEntriesResponse? = nil
        peer : PeerSession? = nil
        stepped_down = false
        stepped_down_term = 0_u64
        has_committed = false

        @lock.synchronize do
          return if @closed || @isolated
          peer = @peers[peer_id]?

          if req.term < @log.current_term
            resp = AppendEntriesResponse.new(@log.current_term, @node_id, false, @log.last_lsn)
          else
            @cluster_configured = true
            @last_leader_contact = Time.instant
            if req.term > @log.current_term || (@role != Role::Follower && @role != Role::Learner)
              stepped_down = step_down_unsafe(req.term, req.leader_id)
              stepped_down_term = req.term
            else
              @leader_id = req.leader_id
            end

            ok, match_lsn, conflict_lsn, conflict_term = @log.maybe_append(
              req.prev_log_lsn,
              req.prev_log_term,
              req.leader_commit,
              req.entries
            )
            has_committed = @last_applied < @log.commit_index
            resp = AppendEntriesResponse.new(@log.current_term, @node_id, ok, match_lsn, conflict_lsn, conflict_term)
          end
        end

        drain_pending_truncations
        @on_step_down.try &.call(stepped_down_term) if stepped_down
        signal_apply if has_committed
        if (r = resp) && (p = peer)
          p.send_append_entries_response(r)
        end
        resp
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def handle_append_entries_response(peer_id : UInt64, resp : AppendEntriesResponse) : Nil
        stepped_down = false
        stepped_down_term = 0_u64
        learner_to_promote : MemberInfo? = nil
        has_committed = false

        @lock.synchronize do
          return if @closed || @isolated
          if resp.term > @log.current_term
            stepped_down = step_down_unsafe(resp.term)
            stepped_down_term = resp.term
            return
          end
          return unless @role.leader? && resp.term == @log.current_term

          @last_ack_at[resp.follower_id] = Time.instant
          @peer_in_flight_until.delete(resp.follower_id)

          if resp.success?
            prev_match = @match_index[resp.follower_id]? || 0_u64
            if resp.match_lsn > prev_match
              @match_index[resp.follower_id] = resp.match_lsn
            end
            @next_index[resp.follower_id] = (@match_index[resp.follower_id]? || 0_u64) + 1
            try_advance_commit_index_unsafe
            has_committed = @last_applied < @log.commit_index ||
                            (@log.commit_index > 0 && @commit_waiters.any? { |w| w.target_lsn <= @log.commit_index })

            if @auto_promote_learners && @pending_conf_change_lsn <= @log.commit_index
              if member = @log.members.find { |m| m.node_id == resp.follower_id && m.learner? }
                lag = @log.last_lsn > resp.match_lsn ? (@log.last_lsn - resp.match_lsn) : 0_u64
                if lag <= @learner_promote_threshold
                  learner_to_promote = MemberInfo.new(
                    member.node_id,
                    member.peer_uri,
                    member.amqp_uri,
                    member.http_uri,
                    MemberRole::Voter
                  )
                end
              end
            end

            if (@next_index[resp.follower_id]? || 1_u64) <= @log.last_lsn ||
               (@peer_last_sent_commit[resp.follower_id]? || 0_u64) < @log.commit_index
              signal_peer_unsafe(resp.follower_id, force: false)
            end
          else
            next_lsn = resp.conflict_lsn > 0 ? resp.conflict_lsn : (@next_index[resp.follower_id]? || 1_u64) - 1
            @next_index[resp.follower_id] = Math.max(1_u64, next_lsn)
            signal_peer_unsafe(resp.follower_id, force: true)
          end
        end

        if stepped_down
          drain_pending_truncations
          @on_step_down.try &.call(stepped_down_term)
        end
        signal_apply if has_committed

        if promoted = learner_to_promote
          begin
            propose_conf_change(ConfChangeType::PromoteLearner, promoted)
          rescue Error::NotLeaderError
          end
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def handle_install_snapshot(peer_id : UInt64, req : InstallSnapshotRequest) : Nil
        resp : InstallSnapshotResponse? = nil
        peer : PeerSession? = nil
        stepped_down = false
        stepped_down_term = 0_u64
        should_install = false

        @lock.synchronize do
          return if @closed || @isolated
          peer = @peers[peer_id]?
          if req.term < @log.current_term
            resp = InstallSnapshotResponse.new(@log.current_term, @node_id, @log.commit_index)
          else
            @cluster_configured = true
            @last_leader_contact = Time.instant
            if req.term > @log.current_term || (@role != Role::Follower && @role != Role::Learner)
              stepped_down = step_down_unsafe(req.term, req.leader_id)
              stepped_down_term = req.term
            else
              @leader_id = req.leader_id
            end

            if req.last_included_lsn > @log.commit_index
              should_install = true
            end
            resp = InstallSnapshotResponse.new(@log.current_term, @node_id, req.last_included_lsn)
          end
        end

        if stepped_down
          drain_pending_truncations
          @on_step_down.try &.call(stepped_down_term)
        end

        if should_install
          @apply_lock.synchronize do
            drain_pending_truncations
            normalized_ctrl = Array(LogEntryRecord).new(req.control_records.size)
            req.control_records.each do |rec|
              follower_rec = strip_leader_flag(rec)
              normalized_ctrl << follower_rec
              begin
                @on_apply.try &.call(follower_rec)
              rescue ex
                Log.error(exception: ex) { "Error applying snapshot control record lsn=#{rec.lsn} cmd=#{rec.command}" }
              end
            end
            begin
              @snapshot_installer.try &.call(req)
            rescue ex
              Log.error(exception: ex) { "Error installing snapshot files at lsn=#{req.last_included_lsn}" }
            end
            @control_history = normalized_ctrl
            save_control_history_under_apply_lock
            @lock.synchronize do
              @log.install_snapshot_state(req.last_included_lsn, req.last_included_term, req.members)
              if req.last_included_lsn > @last_applied
                @last_applied = req.last_included_lsn
                @log.advance_applied_index(@last_applied)
              end
              sync_peer_sessions_unsafe
            end
          end
          signal_apply
        end

        if (r = resp) && (p = peer)
          p.send_install_snapshot_response(r)
        end
      end

      private def handle_install_snapshot_response(peer_id : UInt64, resp : InstallSnapshotResponse) : Nil
        stepped_down = false
        stepped_down_term = 0_u64
        has_committed = false

        @lock.synchronize do
          return if @closed || @isolated
          @snapshot_in_flight.delete(resp.follower_id)
          @peer_in_flight_until.delete(resp.follower_id)
          if resp.term > @log.current_term
            stepped_down = step_down_unsafe(resp.term)
            stepped_down_term = resp.term
            return
          end
          return unless @role.leader? && resp.term == @log.current_term

          @last_ack_at[resp.follower_id] = Time.instant
          @match_index[resp.follower_id] = Math.max(@match_index[resp.follower_id]? || 0_u64, resp.match_lsn)
          @next_index[resp.follower_id] = @match_index[resp.follower_id] + 1
          try_advance_commit_index_unsafe
          has_committed = @last_applied < @log.commit_index ||
                          (@log.commit_index > 0 && @commit_waiters.any? { |w| w.target_lsn <= @log.commit_index })
          signal_peer_unsafe(peer_id, force: true)
        end

        if stepped_down
          drain_pending_truncations
          @on_step_down.try &.call(stepped_down_term)
        end
        signal_apply if has_committed
      end

      private def replicate_to_peers(force : Bool = false) : Nil
        @lock.synchronize do
          return if !@role.leader? || @closed || @isolated
          sync_peer_sessions_unsafe
          signal_all_peers_unsafe(force: force)
        end
      end

      private def signal_all_peers_unsafe(force : Bool) : Nil
        @peers.each_key do |pid|
          signal_peer_unsafe(pid, force: force)
        end
      end

      private def signal_peer_unsafe(peer_id : UInt64, force : Bool) : Nil
        @peer_force_send[peer_id] = true if force
        if ch = @peer_notify[peer_id]?
          ch.try_send?(true)
        end
      end

      private def peer_replication_loop(peer_id : UInt64, notify : ::Channel(Bool)) : Nil
        loop do
          notify.receive? || break
          break if @closed
          replicate_to_peer_now(peer_id)
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def replicate_to_peer_now(peer_id : UInt64) : Nil
        append_req : AppendEntriesRequest? = nil
        leased_mfiles : Array(MFile)? = nil
        want_snapshot = false
        peer : PeerSession? = nil

        @lock.synchronize do
          return if !@role.leader? || @closed || @isolated
          peer = @peers[peer_id]? || return
          return if peer.paused?

          now = Time.instant
          force = @peer_force_send.delete(peer_id) || false
          next_lsn = @next_index[peer_id]? || (@log.last_lsn + 1)

          if next_lsn < @log.first_lsn
            if started_at = @snapshot_in_flight[peer_id]?
              return if (now - started_at) < 30.seconds
            end
            @snapshot_in_flight[peer_id] = now
            want_snapshot = true
          else
            if !force
              if in_flight_until = @peer_in_flight_until[peer_id]?
                return if now < in_flight_until
              end
              has_unsent = next_lsn <= @log.last_lsn
              commit_unsent = (@peer_last_sent_commit[peer_id]? || 0_u64) < @log.commit_index
              return unless has_unsent || commit_unsent
            end

            if leased = @log.read_entries_leased(next_lsn, max_count: 256)
              entries, mfiles = leased
              leased_mfiles = mfiles
            else
              entries = Array(LogEntryRecord).new
            end
            prev_lsn = next_lsn - 1
            prev_term = @log.term_at(prev_lsn) || @log.last_snapshot_term
            commit_idx = @log.commit_index
            append_req = AppendEntriesRequest.new(
              term: @log.current_term,
              leader_id: @node_id,
              prev_log_lsn: prev_lsn,
              prev_log_term: prev_term,
              leader_commit: commit_idx,
              entries: entries
            )
            @peer_in_flight_until[peer_id] = now + @heartbeat_interval
            @peer_last_sent_commit[peer_id] = commit_idx
          end
        end

        begin
          p = peer || return
          if want_snapshot
            sreq = build_consistent_snapshot_request
            unless sreq && p.send_install_snapshot(sreq)
              @lock.synchronize { @snapshot_in_flight.delete(peer_id) }
            end
          elsif areq = append_req
            unless p.send_append_entries(areq)
              @lock.synchronize { @peer_in_flight_until.delete(peer_id) }
            end
          end
        ensure
          leased_mfiles.try &.each(&.release_lease)
        end
      end

      private def build_consistent_snapshot_request : InstallSnapshotRequest?
        @apply_lock.synchronize do
          drain_pending_truncations
          term = 0_u64
          snap_lsn = 0_u64
          snap_term = 0_u64
          members_list = Array(MemberInfo).new
          ok = @lock.synchronize do
            next false if !@role.leader? || @closed || @isolated
            term = @log.current_term
            snap_lsn = @last_applied
            snap_term = @log.term_at(snap_lsn) || @log.last_snapshot_term
            members_list = @log.members.dup
            true
          end
          return unless ok

          ctrl_records = @control_history.dup
          files = begin
            @snapshot_builder.try(&.call(snap_lsn)) || Array(SnapshotSegmentFile).new
          rescue ex
            Log.error(exception: ex) { "Error building snapshot at lsn=#{snap_lsn}" }
            Array(SnapshotSegmentFile).new
          end
          InstallSnapshotRequest.new(
            term: term,
            leader_id: @node_id,
            last_included_lsn: snap_lsn,
            last_included_term: snap_term,
            members: members_list,
            control_records: ctrl_records,
            files: files
          )
        end
      end

      private def try_advance_commit_index_unsafe : Nil
        @quorum_scratch.clear
        @log.members.each do |m|
          next unless m.voter?
          lsn = m.node_id == @node_id ? @log.last_lsn : (@match_index[m.node_id]? || 0_u64)
          @quorum_scratch << lsn
        end
        return if @quorum_scratch.empty?

        # In-place descending insertion sort on pre-allocated buffer (zero heap allocations)
        i = 1
        while i < @quorum_scratch.size
          val = @quorum_scratch[i]
          j = i - 1
          while j >= 0 && @quorum_scratch[j] < val
            @quorum_scratch[j + 1] = @quorum_scratch[j]
            j -= 1
          end
          @quorum_scratch[j + 1] = val
          i += 1
        end

        quorum_idx = @quorum_scratch.size // 2
        quorum_lsn = @quorum_scratch[quorum_idx]

        if quorum_lsn > @log.commit_index && @log.term_at(quorum_lsn) == @log.current_term
          @log.advance_commit_index(quorum_lsn)
        end
      end

      private def signal_apply : Nil
        @apply_notify.try_send?(true)
      end

      private def apply_loop : Nil
        loop do
          @apply_notify.receive? || break
          break if @closed
          drain_committed_callbacks_now
        end
      end

      private def drain_pending_truncations : Nil
        return if @lock.synchronize { @pending_truncations.empty? }
        @apply_lock.synchronize do
          loop do
            entry = @lock.synchronize { @pending_truncations.shift? } || break
            begin
              @on_truncate.try &.call(entry)
            rescue ex
              Log.error(exception: ex) { "Error rolling back truncated entry lsn=#{entry.lsn} cmd=#{entry.command}" }
            end
          end
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def drain_committed_callbacks_now : Nil
        @apply_lock.synchronize do
          loop do
            drain_pending_truncations
            rec_to_apply : LogEntryRecord? = nil
            leased_mfile : MFile? = nil
            promote_term : UInt64? = nil
            is_ctrl = false
            next_lsn = 0_u64

            @lock.synchronize do
              return if @closed
              if @last_applied >= @log.commit_index
                wake_commit_waiters_unsafe
                return
              end

              next_lsn = @last_applied + 1
              if next_lsn < @log.first_lsn
                @last_applied = @log.first_lsn - 1
                @log.advance_applied_index(@last_applied)
                next
              end

              if idx_entry = @log.read_index_entry(next_lsn)
                is_ctrl = control_record?(idx_entry.command)
                if idx_entry.from_leader? && !is_ctrl
                  rec_to_apply = LogEntryRecord.new(idx_entry, Bytes.empty)
                elsif leased = @log.read_entry_leased(next_lsn)
                  rec_to_apply, leased_mfile = leased
                end

                if (rec = rec_to_apply) && rec.command.leader_no_op? && @role.leader? && @promoted_term < rec.term
                  @promoted_term = rec.term
                  promote_term = rec.term
                end
              end
            end

            begin
              if (rec = rec_to_apply) && is_ctrl
                record_control_history_under_apply_lock(rec)
              end

              if pterm = promote_term
                begin
                  @on_promote.try &.call(pterm)
                rescue ex
                  Log.error(exception: ex) { "Error in on_promote callback for term=#{pterm}" }
                end
              end

              if rec = rec_to_apply
                begin
                  @on_apply.try &.call(rec)
                rescue ex
                  Log.error(exception: ex) { "Error applying committed entry lsn=#{rec.lsn} cmd=#{rec.command}" }
                end
              end
            ensure
              leased_mfile.try(&.release_lease)
            end

            @lock.synchronize do
              if next_lsn > @last_applied
                @last_applied = next_lsn
                @log.advance_applied_index(@last_applied)
              end
              wake_commit_waiters_unsafe
            end
          end
        end
      end

      private def wake_commit_waiters_unsafe : Nil
        return unless @log.commit_index > 0
        @commit_waiters.reject! do |w|
          if w.target_lsn <= @last_applied && w.target_lsn <= @log.commit_index
            w.channel.try_send?(true)
            true
          else
            false
          end
        end
      end

      private def control_record?(cmd : LogCommandType) : Bool
        !cmd.publish? && !cmd.ack? && !cmd.tombstone? &&
          !cmd.leader_no_op? && !cmd.segment_delete? &&
          !cmd.stream_offset_commit? && !cmd.queue_purge? &&
          !cmd.conf_change?
      end

      private def strip_leader_flag(rec : LogEntryRecord) : LogEntryRecord
        e = rec.entry
        stripped_entry = LogIndexEntry.new(
          term: e.term,
          lsn: e.lsn,
          vhost_id: e.vhost_id,
          cmd_type: e.cmd_type,
          flags: e.flags & ~LogIndexEntry::FLAG_FROM_LEADER,
          queue_id: e.queue_id,
          segment_id: e.segment_id,
          position: e.position,
          byte_size: e.byte_size
        )
        LogEntryRecord.new(stripped_entry, rec.payload.dup)
      end

      private def extract_lp16_prefix(payload : Bytes) : Bytes
        return Bytes.empty if payload.size < 2
        len = IO::ByteFormat::LittleEndian.decode(UInt16, payload[0, 2]).to_i
        return Bytes.empty if payload.size < 2 + len
        payload[0, 2 + len]
      end

      # Compacts superseded control records and persists `@control_history` outside `RaftNode#@lock`.
      # ameba:disable Metrics/CyclomaticComplexity
      private def record_control_history_under_apply_lock(rec : LogEntryRecord) : Nil
        stored = strip_leader_flag(rec)
        compacted = false
        vhost_id = stored.entry.vhost_id
        queue_id = stored.entry.queue_id

        case stored.command
        when LogCommandType::VHostCreate
          before = @control_history.size
          @control_history.reject! { |r| r.command == LogCommandType::VHostCreate }
          compacted = @control_history.size != before
        when LogCommandType::VHostDelete
          if vhost_id != 0
            before = @control_history.size
            @control_history.reject! { |r| r.entry.vhost_id == vhost_id }
            compacted = @control_history.size != before
          end
        when .queue_delete?
          if queue_id != 0
            before = @control_history.size
            @control_history.reject! do |r|
              r.entry.vhost_id == vhost_id && r.entry.queue_id == queue_id &&
                (r.command.queue_declare? || r.command.queue_delete? || r.command.binding_add? || r.command.binding_remove?)
            end
            compacted = @control_history.size != before
          end
        when .exchange_delete?
          if queue_id != 0
            before = @control_history.size
            @control_history.reject! do |r|
              r.entry.vhost_id == vhost_id && r.entry.queue_id == queue_id &&
                (r.command.exchange_declare? || r.command.exchange_delete?)
            end
            compacted = @control_history.size != before
          end
        when .file_replace?
          prefix = extract_lp16_prefix(stored.payload)
          unless prefix.empty?
            before = @control_history.size
            @control_history.reject! do |r|
              r.command.file_replace? && extract_lp16_prefix(r.payload) == prefix
            end
            compacted = @control_history.size != before
          end
        when .mqtt_retain_put?, .mqtt_retain_delete?
          if queue_id != 0
            before = @control_history.size
            @control_history.reject! do |r|
              r.entry.vhost_id == vhost_id && r.entry.queue_id == queue_id &&
                (r.command.mqtt_retain_put? || r.command.mqtt_retain_delete?)
            end
            compacted = @control_history.size != before
          elsif stored.command.mqtt_retain_put?
            prefix = extract_lp16_prefix(stored.payload)
            unless prefix.empty?
              before = @control_history.size
              @control_history.reject! do |r|
                r.entry.vhost_id == vhost_id && r.command.mqtt_retain_put? &&
                  extract_lp16_prefix(r.payload) == prefix
              end
              compacted = @control_history.size != before
            end
          end
        else
        end

        @control_history << stored
        if compacted || !File.exists?(@ctrl_hist_path)
          save_control_history_under_apply_lock
        else
          append_control_history_under_apply_lock(stored)
        end
      end

      private def load_control_history : Nil
        return unless File.exists?(@ctrl_hist_path)
        File.open(@ctrl_hist_path, "r") do |f|
          return if f.size < 8
          magic = UInt32.from_io(f, IO::ByteFormat::LittleEndian)
          return unless magic == CTRL_HIST_MAGIC
          count = UInt32.from_io(f, IO::ByteFormat::LittleEndian)
          records = Array(LogEntryRecord).new(count)
          count.times do
            records << strip_leader_flag(LogEntryRecord.from_io(f))
          end
          @control_history = records
        end
      rescue
        @control_history.clear
      end

      private def save_control_history_under_apply_lock : Nil
        tmp_path = "#{@ctrl_hist_path}.tmp"
        File.open(tmp_path, "w") do |f|
          f.write_bytes(CTRL_HIST_MAGIC, IO::ByteFormat::LittleEndian)
          f.write_bytes(@control_history.size.to_u32, IO::ByteFormat::LittleEndian)
          @control_history.each(&.to_io(f))
        end
        File.rename(tmp_path, @ctrl_hist_path)
      rescue
      end

      private def append_control_history_under_apply_lock(rec : LogEntryRecord) : Nil
        File.open(@ctrl_hist_path, "r+") do |f|
          if f.size < 8
            f.close
            save_control_history_under_apply_lock
            return
          end
          f.seek(0, IO::Seek::End)
          rec.to_io(f)
          f.seek(4, IO::Seek::Set)
          f.write_bytes(@control_history.size.to_u32, IO::ByteFormat::LittleEndian)
        end
      rescue
        save_control_history_under_apply_lock
      end

      private def on_log_membership_changed_unsafe(members : Array(MemberInfo)) : Nil
        if me = members.find { |m| m.node_id == @node_id }
          if me.learner? && !@role.leader?
            @role = Role::Learner
          elsif me.voter? && @role.learner?
            @role = Role::Follower
          end
        end

        active_ids = members.map(&.node_id)
        @peers.keys.each do |pid|
          unless active_ids.includes?(pid)
            if ch = @peer_notify.delete(pid)
              ch.close
            end
            @peer_force_send.delete(pid)
            @peer_in_flight_until.delete(pid)
            @peer_last_sent_commit.delete(pid)
            @snapshot_in_flight.delete(pid)
            if peer = @peers.delete(pid)
              peer.close
            end
          end
        end

        sync_peer_sessions_unsafe
      end

      private def create_peer_session_unsafe(peer_id : UInt64, peer_uri : String) : PeerSession
        session = PeerSession.new(@node_id, @peer_uri, peer_id, peer_uri, @secret, @execution_context) do |pid, ftype, io|
          handle_peer_frame(pid, ftype, io)
        end
        @peers[peer_id] = session
        unless @peer_notify.has_key?(peer_id)
          ch = ::Channel(Bool).new(1)
          @peer_notify[peer_id] = ch
          @execution_context.spawn(name: "ClusteringV2 replicator #{@node_id}->#{peer_id}") do
            peer_replication_loop(peer_id, ch)
          end
        end
        session
      end

      private def sync_peer_sessions_unsafe : Nil
        @log.members.each do |m|
          next if m.node_id == @node_id
          if existing = @peers[m.node_id]?
            existing.peer_uri = m.peer_uri unless m.peer_uri.empty?
          else
            create_peer_session_unsafe(m.node_id, m.peer_uri)
          end
        end
      end

      private def quorum_size_unsafe : Int32
        voters = @log.members.count(&.voter?)
        (voters // 2) + 1
      end

      private def random_election_timeout : Time::Span
        min_ms = @election_timeout_min.total_milliseconds.to_i64
        max_ms = @election_timeout_max.total_milliseconds.to_i64
        span_ms = max_ms > min_ms ? Random.rand(min_ms..max_ms) : min_ms
        span_ms.milliseconds
      end

      module Error
        class NotLeaderError < Exception
          getter leader_id : UInt64

          def initialize(@leader_id : UInt64)
            super("Node is not the cluster leader (current leader_id=#{@leader_id})")
          end
        end
      end
    end
  end
end
