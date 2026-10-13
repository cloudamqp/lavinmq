require "socket"
require "./protocol"
require "./raft_log"
require "./peer_session"

module LavinMQ
  module ClusteringV2
    class RaftNode
      alias ApplyCallback = LogEntryRecord -> Nil
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
      getter role : Role = Role::Follower
      getter leader_id : UInt64 = 0_u64
      getter last_applied : UInt64 = 0_u64
      property peer_uri : String
      property amqp_uri : String
      property http_uri : String
      property heartbeat_interval : Time::Span = 50.milliseconds
      property election_timeout_min : Time::Span = 200.milliseconds
      property election_timeout_max : Time::Span = 400.milliseconds
      property leader_lease_timeout : Time::Span = 350.milliseconds
      property leader_stickiness_window : Time::Span = 150.milliseconds
      property learner_promote_threshold : UInt64 = 32_u64
      property? auto_promote_learners : Bool = true

      @lock = Mutex.new(:reentrant)
      @peers = Hash(UInt64, PeerSession).new
      @next_index = Hash(UInt64, UInt64).new
      @match_index = Hash(UInt64, UInt64).new
      @last_ack_at = Hash(UInt64, Time::Instant).new
      @pre_votes_granted = Set(UInt64).new
      @votes_granted = Set(UInt64).new
      @commit_waiters = Array(CommitWaiter).new
      @control_history = Array(LogEntryRecord).new
      @last_leader_contact : Time::Instant = Time.instant
      @current_election_timeout : Time::Span = 300.milliseconds
      @promoted_term : UInt64 = 0_u64
      @pending_conf_change_lsn : UInt64 = 0_u64
      @listener : TCPServer?
      @closed = false
      @isolated = false

      @on_apply : ApplyCallback?
      @on_promote : PromoteCallback?
      @on_step_down : StepDownCallback?
      @snapshot_builder : SnapshotBuilderCallback?
      @snapshot_installer : SnapshotInstallerCallback?

      def initialize(@log : RaftLog,
                     @secret : String,
                     @peer_uri : String = "",
                     @amqp_uri : String = "",
                     @http_uri : String = "",
                     initial_role : MemberRole = MemberRole::Voter)
        @node_id = @log.node_id
        @role = initial_role.learner? ? Role::Learner : Role::Follower
        @current_election_timeout = random_election_timeout
        if @log.members.empty? && !@peer_uri.empty?
          @log.set_members([MemberInfo.new(@node_id, @peer_uri, @amqp_uri, @http_uri, initial_role)])
        end
      end

      def on_apply(&block : LogEntryRecord -> Nil) : Nil
        @on_apply = block
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
          my_role = @role.learner? ? MemberRole::Learner : MemberRole::Voter
          if existing = @log.members.find { |m| m.node_id == @node_id }
            my_role = existing.role
          end
          @log.upsert_member(MemberInfo.new(@node_id, @peer_uri, @amqp_uri, @http_uri, my_role))
          sync_peer_sessions_unsafe
          @last_leader_contact = Time.instant
        end

        spawn(name: "ClusteringV2 listener #{@node_id}") { accept_loop(server) }
        spawn(name: "ClusteringV2 ticker #{@node_id}") { tick_loop }
      end

      def configure_cluster(members : Array(MemberInfo)) : Nil
        @lock.synchronize do
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
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_publish(@log.current_term, vhost_id, queue_id, segment_id, position, byte_size, fallback_payload)
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          e
        end
        replicate_to_peers
        drain_committed_callbacks
        entry
      end

      def propose_ack(vhost_id : UInt16,
                      queue_id : UInt32,
                      segment_id : UInt32,
                      position : UInt32) : LogIndexEntry
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_ack(@log.current_term, vhost_id, queue_id, segment_id, position)
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          e
        end
        replicate_to_peers
        drain_committed_callbacks
        entry
      end

      def propose_segment_delete(vhost_id : UInt16,
                                 queue_id : UInt32,
                                 segment_id : UInt32) : LogIndexEntry
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_segment_delete(@log.current_term, vhost_id, queue_id, segment_id)
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          e
        end
        replicate_to_peers
        drain_committed_callbacks
        entry
      end

      def propose_command(cmd_type : LogCommandType,
                          payload : Bytes = Bytes.empty,
                          vhost_id : UInt16 = 0_u16,
                          queue_id : UInt32 = 0_u32,
                          segment_id : UInt32 = 0_u32,
                          position : UInt32 = 0_u32) : LogIndexEntry
        entry = @lock.synchronize do
          ensure_leader_unsafe!
          e = @log.append_command(@log.current_term, cmd_type, payload, vhost_id, queue_id, segment_id, position)
          if cmd_type.conf_change?
            @pending_conf_change_lsn = e.lsn
            apply_conf_change_bytes_unsafe(payload)
          end
          @match_index[@node_id] = e.lsn
          try_advance_commit_index_unsafe
          e
        end
        replicate_to_peers
        drain_committed_callbacks
        entry
      end

      # Blocks the caller until `target_lsn <= commit_index`, or returns false if
      # the node steps down / times out before committing `target_lsn`.
      def wait_for_commit(target_lsn : UInt64, timeout : Time::Span? = nil) : Bool
        ch = @lock.synchronize do
          return true if @log.commit_index >= target_lsn
          return false unless @role.leader?
          c = ::Channel(Bool).new(1)
          @commit_waiters << CommitWaiter.new(target_lsn, c)
          c
        end

        replicate_to_peers

        if t = timeout
          select
          when res = ch.receive?
            res || false
          when timeout(t)
            @lock.synchronize { @commit_waiters.reject! { |w| w.channel == ch } }
            false
          end
        else
          ch.receive? || false
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
        end
      end

      def pause_peer(peer_id : UInt64, paused : Bool = true) : Nil
        @lock.synchronize do
          if peer = @peers[peer_id]?
            peer.paused = paused
          end
        end
      end

      def trigger_election! : Nil
        start_pre_vote
      end

      def close : Nil
        @lock.synchronize do
          return if @closed
          @closed = true
          @listener.try &.close rescue nil
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

      private def accept_loop(server : TCPServer) : Nil
        loop do
          break if @closed
          sock = server.accept? || break
          sock.tcp_nodelay = true
          sock.sync = false
          spawn(name: "ClusteringV2 inbound handshake") do
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
                s = PeerSession.new(@node_id, @peer_uri, remote_id, remote_uri, @secret) do |pid, ftype, io|
                  handle_peer_frame(pid, ftype, io)
                end
                @peers[remote_id] = s
              end
              s
            end
            session.attach_inbound_socket(sock)
          rescue
            sock.close rescue nil
          end
        end
      end

      private def tick_loop : Nil
        loop do
          sleep 15.milliseconds
          break if @closed

          should_replicate = false
          should_pre_vote = false
          stepped_down_term : UInt64? = nil

          @lock.synchronize do
            return if @closed
            now = Time.instant

            case @role
            when .leader?
              if check_leader_lease_expired_unsafe(now)
                stepped_down_term = @log.current_term
                step_down_unsafe(@log.current_term)
              else
                should_replicate = true
              end
            when .follower?, .pre_candidate?, .candidate?
              if voting_single_node_cluster_unsafe?
                become_candidate_and_win_single_node_unsafe
              elsif now - @last_leader_contact >= @current_election_timeout
                should_pre_vote = true
              end
            when .learner?
              # Learners do not initiate elections
            end
          end

          if term = stepped_down_term
            @on_step_down.try &.call(term)
          elsif should_replicate
            replicate_to_peers
            drain_committed_callbacks
          elsif should_pre_vote
            start_pre_vote
          end
        end
      end

      private def voting_single_node_cluster_unsafe? : Bool
        voters = @log.members.select(&.voter?)
        voters.size == 1 && voters.first.node_id == @node_id
      end

      private def become_candidate_and_win_single_node_unsafe : Nil
        new_term = @log.current_term + 1
        @log.set_term_and_vote(new_term, @node_id)
        become_leader_unsafe
      end

      private def check_leader_lease_expired_unsafe(now : Time::Instant) : Bool
        voters = @log.members.select(&.voter?)
        return false if voters.size <= 1

        quorum = (voters.size // 2) + 1
        active = 1 # self
        voters.each do |m|
          next if m.node_id == @node_id
          if last_ack = @last_ack_at[m.node_id]?
            active += 1 if (now - last_ack) <= @leader_lease_timeout
          end
        end
        active < quorum
      end

      private def start_pre_vote : Nil
        req : VoteRequest? = nil
        peers_to_send = Array(PeerSession).new

        @lock.synchronize do
          return if @closed || @role.leader? || @role.learner? || @isolated
          if voting_single_node_cluster_unsafe?
            become_candidate_and_win_single_node_unsafe
            return
          end

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
          peers_to_send = @peers.values
        end

        drain_committed_callbacks

        if vote_req = req
          peers_to_send.each do |peer|
            spawn { peer.send_vote_request(vote_req) }
          end
        end
      end

      private def start_real_election_unsafe : Tuple(VoteRequest?, Array(PeerSession))
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
        {req, @peers.values}
      end

      private def become_leader_unsafe : Nil
        @role = Role::Leader
        @leader_id = @node_id
        now = Time.instant
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
          leader_recently_active = @leader_id != 0 && @leader_id != req.candidate_id &&
                                   (now - @last_leader_contact) < @leader_stickiness_window

          if req.pre_vote?
            grant = !leader_recently_active &&
                    req.term >= @log.current_term &&
                    @log.up_to_date?(req.last_log_lsn, req.last_log_term)
            resp = VoteResponse.new(true, @log.current_term, @node_id, grant)
          else
            if req.term > @log.current_term && !leader_recently_active
              stepped_down = step_down_unsafe(req.term)
              stepped_down_term = req.term
            end

            can_vote = (@log.voted_for == 0 || @log.voted_for == req.candidate_id)
            grant = !leader_recently_active &&
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

        @on_step_down.try &.call(stepped_down_term) if stepped_down
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

        @on_step_down.try &.call(stepped_down_term) if stepped_down
        if req = real_vote_req
          peers_to_send.each do |peer|
            spawn { peer.send_vote_request(req) }
          end
        end
        if became_leader
          replicate_to_peers
          drain_committed_callbacks
        end
      end

      private def handle_append_entries(peer_id : UInt64, req : AppendEntriesRequest) : Nil
        resp : AppendEntriesResponse? = nil
        peer : PeerSession? = nil
        stepped_down = false
        stepped_down_term = 0_u64

        @lock.synchronize do
          return if @closed || @isolated
          peer = @peers[peer_id]?

          if req.term < @log.current_term
            resp = AppendEntriesResponse.new(@log.current_term, @node_id, false, @log.last_lsn)
          else
            @last_leader_contact = Time.instant
            if req.term > @log.current_term || !@role.follower?
              stepped_down = step_down_unsafe(req.term, req.leader_id)
              stepped_down_term = req.term
            else
              @leader_id = req.leader_id
            end

            req.entries.each do |rec|
              if rec.command.conf_change?
                apply_conf_change_bytes_unsafe(rec.payload)
              end
            end

            ok, match_lsn, conflict_lsn, conflict_term = @log.maybe_append(
              req.prev_log_lsn,
              req.prev_log_term,
              req.leader_commit,
              req.entries
            )
            resp = AppendEntriesResponse.new(@log.current_term, @node_id, ok, match_lsn, conflict_lsn, conflict_term)
          end
        end

        @on_step_down.try &.call(stepped_down_term) if stepped_down
        drain_committed_callbacks
        if (r = resp) && (p = peer)
          p.send_append_entries_response(r)
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def handle_append_entries_response(peer_id : UInt64, resp : AppendEntriesResponse) : Nil
        stepped_down = false
        stepped_down_term = 0_u64
        needs_retry = false
        learner_to_promote : MemberInfo? = nil

        @lock.synchronize do
          return if @closed || @isolated
          if resp.term > @log.current_term
            stepped_down = step_down_unsafe(resp.term)
            stepped_down_term = resp.term
            return
          end
          return unless @role.leader? && resp.term == @log.current_term

          @last_ack_at[resp.follower_id] = Time.instant

          if resp.success?
            prev_match = @match_index[resp.follower_id]? || 0_u64
            if resp.match_lsn > prev_match
              @match_index[resp.follower_id] = resp.match_lsn
            end
            @next_index[resp.follower_id] = (@match_index[resp.follower_id]? || 0_u64) + 1
            try_advance_commit_index_unsafe

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
          else
            next_lsn = resp.conflict_lsn > 0 ? resp.conflict_lsn : (@next_index[resp.follower_id]? || 1_u64) - 1
            @next_index[resp.follower_id] = Math.max(1_u64, next_lsn)
            needs_retry = true
          end
        end

        @on_step_down.try &.call(stepped_down_term) if stepped_down
        drain_committed_callbacks

        if promoted = learner_to_promote
          begin
            propose_conf_change(ConfChangeType::PromoteLearner, promoted)
          rescue Error::NotLeaderError
          end
        elsif needs_retry
          replicate_to_peer(peer_id)
        end
      end

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
            resp = InstallSnapshotResponse.new(@log.current_term, @node_id, @log.last_lsn)
          else
            @last_leader_contact = Time.instant
            if req.term > @log.current_term || !@role.follower?
              stepped_down = step_down_unsafe(req.term, req.leader_id)
              stepped_down_term = req.term
            else
              @leader_id = req.leader_id
            end

            if req.last_included_lsn > @log.commit_index
              should_install = true
            end
            resp = InstallSnapshotResponse.new(@log.current_term, @node_id, Math.max(@log.last_lsn, req.last_included_lsn))
          end
        end

        @on_step_down.try &.call(stepped_down_term) if stepped_down

        if should_install
          req.control_records.each do |rec|
            @on_apply.try &.call(rec)
          end
          @snapshot_installer.try &.call(req)
          @lock.synchronize do
            @log.install_snapshot_state(req.last_included_lsn, req.last_included_term, req.members)
            @last_applied = Math.max(@last_applied, req.last_included_lsn)
            sync_peer_sessions_unsafe
          end
        end

        if (r = resp) && (p = peer)
          p.send_install_snapshot_response(r)
        end
      end

      private def handle_install_snapshot_response(peer_id : UInt64, resp : InstallSnapshotResponse) : Nil
        stepped_down = false
        stepped_down_term = 0_u64

        @lock.synchronize do
          return if @closed || @isolated
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
        end

        @on_step_down.try &.call(stepped_down_term) if stepped_down
        drain_committed_callbacks
        replicate_to_peer(peer_id)
      end

      private def replicate_to_peers : Nil
        peer_ids = @lock.synchronize do
          return if !@role.leader? || @closed || @isolated
          sync_peer_sessions_unsafe
          @peers.keys
        end
        peer_ids.each { |pid| replicate_to_peer(pid) }
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def replicate_to_peer(peer_id : UInt64) : Nil
        append_req : AppendEntriesRequest? = nil
        snap_req : InstallSnapshotRequest? = nil
        peer : PeerSession? = nil

        @lock.synchronize do
          return if !@role.leader? || @closed || @isolated
          peer = @peers[peer_id]? || return
          next_lsn = @next_index[peer_id]? || (@log.last_lsn + 1)

          if next_lsn < @log.first_lsn
            snap_lsn = @log.commit_index
            snap_term = @log.term_at(snap_lsn) || @log.last_snapshot_term
            files = @snapshot_builder.try(&.call(snap_lsn)) || Array(SnapshotSegmentFile).new
            snap_req = InstallSnapshotRequest.new(
              term: @log.current_term,
              leader_id: @node_id,
              last_included_lsn: snap_lsn,
              last_included_term: snap_term,
              members: @log.members,
              control_records: @control_history.dup,
              files: files
            )
          else
            entries = @log.read_entries(next_lsn, max_count: 256) || Array(LogEntryRecord).new
            prev_lsn = next_lsn - 1
            prev_term = @log.term_at(prev_lsn) || @log.last_snapshot_term
            append_req = AppendEntriesRequest.new(
              term: @log.current_term,
              leader_id: @node_id,
              prev_log_lsn: prev_lsn,
              prev_log_term: prev_term,
              leader_commit: @log.commit_index,
              entries: entries
            )
          end
        end

        if p = peer
          if sreq = snap_req
            spawn { p.send_install_snapshot(sreq) }
          elsif areq = append_req
            spawn { p.send_append_entries(areq) }
          end
        end
      end

      private def try_advance_commit_index_unsafe : Nil
        voters = @log.members.select(&.voter?)
        return if voters.empty?

        match_lsns = voters.map do |m|
          m.node_id == @node_id ? @log.last_lsn : (@match_index[m.node_id]? || 0_u64)
        end
        match_lsns.sort! { |a, b| b <=> a }
        quorum_idx = (voters.size // 2)
        quorum_lsn = match_lsns[quorum_idx]

        if quorum_lsn > @log.commit_index && @log.term_at(quorum_lsn) == @log.current_term
          @log.advance_commit_index(quorum_lsn)
        end
      end

      # ameba:disable Metrics/CyclomaticComplexity
      private def drain_committed_callbacks : Nil
        to_apply = Array(LogEntryRecord).new
        promote_term : UInt64? = nil
        woken_waiters = Array(CommitWaiter).new

        @lock.synchronize do
          while @last_applied < @log.commit_index
            next_lsn = @last_applied + 1
            if next_lsn < @log.first_lsn
              @last_applied = @log.first_lsn - 1
              next
            end
            if rec = @log.read_entry(next_lsn)
              to_apply << rec
              if rec.command.leader_no_op? && @role.leader? && @promoted_term < rec.term
                @promoted_term = rec.term
                promote_term = rec.term
              elsif !rec.command.publish? && !rec.command.ack? && !rec.command.tombstone? &&
                    !rec.command.leader_no_op? && !rec.command.segment_delete? &&
                    !rec.command.stream_offset_commit? && !rec.command.queue_purge?
                @control_history << rec
              end
            end
            @last_applied = next_lsn
          end

          if @log.commit_index > 0
            @commit_waiters.reject! do |w|
              if w.target_lsn <= @log.commit_index
                woken_waiters << w
                true
              else
                false
              end
            end
          end
        end

        if pterm = promote_term
          @on_promote.try &.call(pterm)
        end

        to_apply.each do |rec|
          @on_apply.try &.call(rec)
        end

        woken_waiters.each do |w|
          w.channel.try_send(true)
        end
      end

      private def apply_conf_change_bytes_unsafe(payload : Bytes) : Nil
        return if payload.empty?
        cc = ConfChangeCommand.from_slice(payload)
        case cc.change_type
        in .add_learner?, .promote_learner?
          @log.upsert_member(cc.member)
          if cc.member.node_id == @node_id
            if cc.member.voter? && @role.learner?
              @role = Role::Follower
            elsif cc.member.learner? && !@role.leader?
              @role = Role::Learner
            end
          end
        in .remove_node?
          @log.remove_member(cc.member.node_id)
          if peer = @peers.delete(cc.member.node_id)
            peer.close
          end
        end
        sync_peer_sessions_unsafe
      end

      private def sync_peer_sessions_unsafe : Nil
        @log.members.each do |m|
          next if m.node_id == @node_id
          if existing = @peers[m.node_id]?
            existing.peer_uri = m.peer_uri unless m.peer_uri.empty?
          else
            @peers[m.node_id] = PeerSession.new(@node_id, @peer_uri, m.node_id, m.peer_uri, @secret) do |pid, ftype, io|
              handle_peer_frame(pid, ftype, io)
            end
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
