require "./membership"
require "./state"
require "./control_mesh"
require "./messages"

module LavinMQ
  module Clustering
    module VR
      # The Viewstamped Replication consensus FSM. Replaces etcd for leader
      # election and failure detection. Each node runs one Node; they talk over
      # the control mesh. The Node decides who the primary is for the current
      # view and signals role transitions through callbacks:
      #
      #   on_primary     — this node has become primary; start serving (once).
      #   on_new_primary — a *different* node is primary; (re-)follow it.
      #   on_step_down   — this node was primary and has been deposed; per the
      #                    project's decision the process exits and restarts as a
      #                    backup (no in-process demotion).
      #
      # The data layer supplies the log head and commit point through `op_source`
      # / `commit_source`, so the FSM stays independent of replication mechanics
      # and is unit-testable with stubs.
      class Node
        Log = LavinMQ::Log.for "clustering.vr.node"

        enum Status
          Normal
          ViewChange
        end

        record LogSummary, last_normal_view : UInt64, op : UInt64, commit_op : UInt64 do
          include Comparable(LogSummary)

          # Most-up-to-date ordering: higher last_normal_view wins, then higher op.
          def <=>(other : LogSummary)
            {last_normal_view, op} <=> {other.last_normal_view, other.op}
          end
        end

        getter status : Status
        getter primary_id : Int32?

        def initialize(@membership : Membership, @mesh : ControlMesh, @state : State,
                       @heartbeat_interval : Time::Span, @view_change_timeout : Time::Span,
                       @op_source : -> UInt64 = -> { 0u64 },
                       @commit_source : -> UInt64 = -> { 0u64 },
                       @on_primary : -> = -> { },
                       @on_new_primary : Member -> = ->(_m : Member) { },
                       @on_step_down : -> = -> { })
          @status = Status::Normal
          @last_normal_view = @state.view
          @commit_op = @state.commit_op
          @primary_id = nil
          @lock = Mutex.new(:unchecked)
          # Per-view vote tallies during a view change.
          @svc_votes = Hash(UInt64, Set(Int32)).new
          @dvc_votes = Hash(UInt64, Hash(Int32, LogSummary)).new
          @sent_dvc = Set(UInt64).new
          @last_heard = Time.instant
          @became_primary = ::Channel(Nil).new
          # Buffered: the step-down signal must not be lost if it fires while
          # run() is still inside its yield block (serving) and not yet parked on
          # wait_until_stepped_down — otherwise a deposed primary would keep
          # serving forever.
          @stepped_down = ::Channel(Nil).new(1)
          @ever_primary = false
          @stepping_down = false
          @quorum_lost_since = nil.as(Time::Instant?)
          @closed = false
        end

        def self_id : Int32
          @membership.self_id
        end

        def view : UInt64
          @state.view
        end

        def primary? : Bool
          @primary_id == self_id
        end

        # A snapshot of this node's clustering role for the HTTP status endpoint
        # (how the cluster's current primary is discovered now that etcd is gone).
        def status : NamedTuple(node_id: Int32, role: String, view: UInt64, op: UInt64, commit_op: UInt64, primary_id: Int32?, primary_uri: String?)
          @lock.synchronize do
            role = if primary?
                     "primary"
                   elsif @status.view_change?
                     "view_change"
                   else
                     "backup"
                   end
            {
              node_id:     self_id,
              role:        role,
              view:        @state.view,
              op:          @op_source.call,
              commit_op:   current_commit,
              primary_id:  @primary_id,
              primary_uri: @primary_id.try { |pid| @membership.uri_for(pid) },
            }
          end
        end

        # Start the inbound-message and timer fibers. A fresh view-0 cluster's
        # Every node starts as a backup with no primary; the first view-change
        # timeout drives the initial election through the same path as failover
        # (no special bootstrap), so a node only ever becomes primary after a
        # quorum view change — never prematurely.
        def start : Nil
          spawn(name: "VR inbound") { inbound_loop }
          spawn(name: "VR timer") { timer_loop }
        end

        # Block until this node becomes primary (the analogue of winning the etcd
        # election). Returns immediately if already primary.
        def wait_until_primary : Nil
          return if primary?
          @became_primary.receive
        rescue ::Channel::ClosedError
        end

        # Block until this node (having been primary) is deposed.
        def wait_until_stepped_down : Nil
          @stepped_down.receive
        rescue ::Channel::ClosedError
        end

        def close : Nil
          @closed = true
          @became_primary.close
          @stepped_down.close
        end

        # Demand a new election. Called when the data layer rejects the current
        # primary as unfit (e.g. its full_sync baseline is behind our committed
        # data — Client#sync raises BehindLeaderError). Without this a refusing
        # follower would reconnect to the same behind primary forever; starting a
        # view change lets a more-complete node (often this one) take over.
        def request_view_change : Nil
          @lock.synchronize do
            start_view_change(@state.view + 1) unless primary?
          end
        end

        private def inbound_loop : Nil
          while msg = @mesh.inbound.receive?
            @lock.synchronize { handle(msg) }
          end
        rescue ::Channel::ClosedError
        end

        private def timer_loop : Nil
          tick = {@heartbeat_interval, @view_change_timeout / 3}.min
          until @closed
            sleep tick
            @lock.synchronize { on_tick }
          end
        end

        private def on_tick : Nil
          if primary?
            # Fence against split-brain: a primary that can no longer reach a
            # quorum of the mesh must step down, or the partitioned-away majority
            # elects a new primary while this one keeps serving reads / unconfirmed
            # writes as a stale leader. This is the liveness guarantee the etcd
            # lease used to provide. Tolerate a brief blip — only step down after
            # connectivity has been below quorum for a full view-change timeout.
            if quorum_reachable?
              @quorum_lost_since = nil
            else
              since = (@quorum_lost_since ||= Time.instant)
              if Time.instant - since > @view_change_timeout
                step_down("lost contact with a quorum of the cluster")
                return
              end
            end
            @mesh.broadcast(Control::Heartbeat.new(view: @state.view, op: @op_source.call,
              commit_op: current_commit, from_id: self_id))
          elsif @status.normal?
            if Time.instant - @last_heard > jittered_timeout
              start_view_change(@state.view + 1)
            end
          else # ViewChange: re-advertise candidacy in case messages were lost. The
            # control channel drops on overflow, so resend both our StartViewChange
            # and (once we've reached the SVC quorum) our DoViewChange — a one-shot
            # DVC that was dropped would otherwise stall the election until the next
            # view-change timeout.
            @mesh.broadcast(Control::StartViewChange.new(view: @state.view, from_id: self_id))
            resend_do_view_change(@state.view)
          end
        end

        # Can this node still reach a quorum (itself + connected mesh peers)? A
        # single-node cluster (quorum 1) is always reachable.
        private def quorum_reachable? : Bool
          @mesh.connected_ids.size + 1 >= @membership.quorum
        end

        # Spread elections out so peers don't all fire simultaneously; derived
        # from the node id (no RNG, which is unavailable in this codebase's hot
        # paths and would also break determinism).
        private def jittered_timeout : Time::Span
          @view_change_timeout + (self_id.abs % 50).milliseconds
        end

        private def handle(msg : Control::Message) : Nil
          case msg
          when Control::Heartbeat       then on_heartbeat(msg)
          when Control::StartViewChange then on_start_view_change(msg)
          when Control::DoViewChange    then on_do_view_change(msg)
          when Control::StartView       then on_start_view(msg)
          end
        end

        private def on_heartbeat(hb : Control::Heartbeat) : Nil
          return if hb.view < @state.view
          if hb.view > @state.view
            # We missed this view's StartView; adopt the sender as primary.
            become_backup(hb.view, hb.from_id)
          elsif !primary? && (@primary_id.nil? || @status.view_change?)
            # Same view, but we don't yet follow this primary: either learning its
            # identity after a restart, or the view change for this view already
            # completed (only one node can be primary of a view) and we missed the
            # StartView. become_backup leaves ViewChange and clears the vote tally,
            # so we stop campaigning for an already-decided view (which would
            # otherwise wedge us re-broadcasting StartViewChange forever).
            become_backup(hb.view, hb.from_id)
          end
          return unless @primary_id == hb.from_id
          @last_heard = Time.instant
          learn_commit(hb.commit_op)
        end

        private def on_start_view_change(svc : Control::StartViewChange) : Nil
          return if svc.view <= @state.view && @status.normal?
          start_view_change(svc.view) if svc.view > @state.view
          votes = (@svc_votes[svc.view] ||= Set(Int32).new)
          votes << svc.from_id
          votes << self_id
          maybe_send_do_view_change(svc.view)
        end

        # Only the deterministic primary_of(view) collects DoViewChange and
        # decides — so the winner is computed once, from one quorum, and every
        # node learns it from one StartView (no divergent local decisions).
        private def on_do_view_change(dvc : Control::DoViewChange) : Nil
          return if dvc.view < @state.view
          start_view_change(dvc.view) if dvc.view > @state.view
          return unless @membership.primary_of(dvc.view).id == self_id
          tally = (@dvc_votes[dvc.view] ||= Hash(Int32, LogSummary).new)
          tally[dvc.from_id] = LogSummary.new(dvc.last_normal_view, dvc.op, dvc.commit_op)
          tally[self_id] = own_summary
          maybe_decide_view(dvc.view)
        end

        private def on_start_view(sv : Control::StartView) : Nil
          return if sv.view < @state.view
          learn_commit(sv.commit_op)
          install_view(sv.view, sv.primary_id)
        end

        private def start_view_change(new_view : UInt64) : Nil
          return if new_view <= @state.view && @status.view_change?
          @status = Status::ViewChange
          @state.save(view: new_view) # fence: the new view is durable before we vote
          @primary_id = nil
          @sent_dvc.delete(new_view)
          votes = (@svc_votes[new_view] ||= Set(Int32).new)
          votes << self_id
          Log.info { "Starting view change to view #{new_view}" }
          @mesh.broadcast(Control::StartViewChange.new(view: new_view, from_id: self_id))
          maybe_send_do_view_change(new_view)
        end

        # Once a quorum wants `v`, send our log summary to v's deterministic
        # primary (which may be us — then we tally it locally).
        private def maybe_send_do_view_change(v : UInt64) : Nil
          return if @sent_dvc.includes?(v)
          votes = @svc_votes[v]?
          return unless votes && votes.size >= @membership.quorum
          @sent_dvc << v
          send_do_view_change(v)
        end

        # Resend our DoViewChange for `v` if we've already reached the StartView-
        # Change quorum. Called every tick while in ViewChange: the control channel
        # drops on overflow, and DoViewChange is otherwise a one-shot, so a dropped
        # send would leave the decider short of a quorum and stall the election
        # until the next view-change timeout. Resending is idempotent — the decider
        # just re-tallies our (unchanged) summary.
        private def resend_do_view_change(v : UInt64) : Nil
          send_do_view_change(v) if @sent_dvc.includes?(v)
        end

        private def send_do_view_change(v : UInt64) : Nil
          s = own_summary
          decider = @membership.primary_of(v).id
          dvc = Control::DoViewChange.new(view: v, last_normal_view: s.last_normal_view,
            op: s.op, commit_op: s.commit_op, from_id: self_id)
          if decider == self_id
            on_do_view_change(dvc)
          else
            @mesh.send_to(decider, dvc)
          end
        end

        # Decider only: with a quorum of DoViewChange, pick the most up-to-date
        # node (tie-break lowest id) as the new primary and announce it.
        private def maybe_decide_view(v : UInt64) : Nil
          return unless @status.view_change?
          tally = @dvc_votes[v]?
          return unless tally && tally.size >= @membership.quorum
          winner_id = tally.max_by { |id, summary| {summary, -id.to_i64} }[0]
          commit = tally.values.max_of(&.commit_op)
          learn_commit(commit)
          Log.info { "Deciding view #{v}: primary is #{winner_id.to_s(36)} from #{tally.map { |id, s| "#{id.to_s(36)}=op#{s.op}" }.join(",")}" }
          @mesh.broadcast(Control::StartView.new(view: v, primary_id: winner_id,
            op: @op_source.call, commit_op: current_commit, from_id: self_id))
          install_view(v, winner_id) # apply our own decision locally
        end

        # Adopt `v` as a normal view with `primary` as its leader, becoming that
        # leader ourselves or following it.
        private def install_view(v : UInt64, primary : Int32) : Nil
          if primary == self_id
            become_primary(v)
          else
            become_backup(v, primary)
          end
        end

        private def become_primary(v : UInt64) : Nil
          return if primary? && @state.view == v
          @state.save(view: v, commit_op: @commit_op)
          @last_normal_view = v
          @status = Status::Normal
          @primary_id = self_id
          @svc_votes.clear
          @dvc_votes.clear
          Log.info { "Became primary for view #{v}" }
          # Announce immediately so backups follow without waiting for the first
          # heartbeat tick.
          @mesh.broadcast(Control::Heartbeat.new(view: v, op: @op_source.call,
            commit_op: current_commit, from_id: self_id))
          unless @ever_primary
            @ever_primary = true
            @on_primary.call
            @became_primary.try_send(nil) rescue nil
          end
        end

        private def become_backup(v : UInt64, primary : Int32) : Nil
          if @ever_primary
            # We were serving as primary and a newer view exists: step down. Per
            # project decision the process exits and rejoins as a backup.
            step_down("deposed (view #{@state.view} -> #{v})")
            return
          end
          @state.save(view: v)
          @last_normal_view = v
          @status = Status::Normal
          @svc_votes.clear
          @dvc_votes.clear
          @last_heard = Time.instant
          adopt_primary(primary)
        end

        # Step down as primary (deposed by a newer view, or partitioned from a
        # quorum). Per the project decision there is no in-process demotion: signal
        # the runner, which exits so the supervisor restarts this node as a backup.
        # Idempotent — only the first call has any effect.
        private def step_down(reason : String) : Nil
          return if @stepping_down
          @stepping_down = true
          Log.fatal { "Stepping down as primary (view #{@state.view}): #{reason}" }
          @stepped_down.try_send(nil) rescue nil
          @on_step_down.call
        end

        private def adopt_primary(primary : Int32) : Nil
          changed = @primary_id != primary
          @primary_id = primary
          return unless changed
          if member = @membership.member?(primary)
            Log.info { "Following primary #{primary.to_s(36)} for view #{@state.view}" }
            @on_new_primary.call(member)
          end
        end

        private def own_summary : LogSummary
          LogSummary.new(@last_normal_view, @op_source.call, current_commit)
        end

        # This node's best knowledge of the commit point: the data layer's actual
        # majority-commit progress (when this node is the serving primary) or the
        # value learned from the primary's heartbeats (when a backup), whichever
        # is higher. The two are tracked separately — @commit_op is heartbeat/
        # view-change driven, @commit_source reads the live Server — so the
        # primary reports its real commit point rather than a stale marker.
        # Advance the in-memory commit point and mirror it into the shared State,
        # so the data client can read the cluster commit_op and refuse to truncate
        # to a leader that's behind it (Client#sync). Monotonic.
        private def learn_commit(c : UInt64) : Nil
          @commit_op = c if c > @commit_op
          @state.note_commit(@commit_op)
        end

        private def current_commit : UInt64
          c = @commit_source.call
          c > @commit_op ? c : @commit_op
        end
      end
    end
  end
end
