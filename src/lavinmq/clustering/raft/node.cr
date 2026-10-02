require "./core"
require "./storage"
require "./transport"
require "../../bool_channel"
require "../../logger"

module LavinMQ::Clustering::Raft
  # Runs a Core in a single fiber: every message, request and tick goes
  # through @events, so the Core needs no locking. State is persisted before
  # any message produced alongside it leaves the node.
  class Node
    Log = LavinMQ::Log.for "clustering.raft"

    private record Propose, isr : Set(Int32), reply : Channel(Bool)
    private record Transfer, reply : Channel(Bool)
    private record Pending, index : Int64, term : Int64, reply : Channel(Bool)
    private alias Event = Message | Propose | Transfer

    # True while this node is the leader and has committed an entry in its
    # term, i.e. it knows the latest committed ISR.
    getter serving = BoolChannel.new(false)
    # Notified (non-blocking, coalesced) whenever `leader_uri` changes.
    getter leader_changed = Channel(Nil).new(1)

    @events = Channel(Event).new(256)
    @pending = Array(Pending).new
    @leader_uri : String? = nil
    @leader = false
    @committed_isr : Set(Int32)? = nil
    @state_lock = Mutex.new
    @stopped = Channel(Nil).new
    @transport : Transport? = nil

    def initialize(id : String, peers : Enumerable(String), node_id : Int32, uri : String,
                   @storage : Storage, election_timeout : Time::Span, heartbeat_interval : Time::Span,
                   @tick = 20.milliseconds, bootstrap = false)
      @core = Core.new(id, peers, node_id, uri, election_timeout, heartbeat_interval,
        Time.instant, @storage.load, bootstrap: bootstrap)
      @committed_isr = @core.committed_isr
    end

    def run(transport : Transport) : Nil
      @transport = transport
      spawn(event_loop, name: "raft node")
    end

    # Called by the transport for every received message.
    def deliver(msg : Message) : Nil
      @events.send msg
    rescue Channel::ClosedError
    end

    def leader_uri : String?
      @state_lock.synchronize { @leader_uri }
    end

    def leader? : Bool
      @state_lock.synchronize { @leader }
    end

    def committed_isr : Set(Int32)?
      @state_lock.synchronize { @committed_isr }
    end

    # Replicate an ISR change. Blocks until committed (true) or until this
    # node stops being the leader of the term it was proposed in (false).
    def propose_isr(isr : Set(Int32)) : Bool
      reply = Channel(Bool).new(1)
      @events.send Propose.new(isr, reply)
      await reply
    rescue Channel::ClosedError
      false
    end

    # Hand leadership over to a caught up in-sync peer, so the cluster fails
    # over without waiting for an election timeout. Returns false when there
    # was nothing to hand over.
    def transfer_leadership : Bool
      reply = Channel(Bool).new(1)
      @events.send Transfer.new(reply)
      await reply
    rescue Channel::ClosedError
      false
    end

    private def await(reply : Channel(Bool)) : Bool
      select
      when result = reply.receive
        result
      when @stopped.receive?
        false
      end
    end

    def close : Nil
      @events.close
      if @transport
        @stopped.receive?
      else
        @stopped.close
      end
      @transport.try &.close
    end

    private def event_loop : Nil
      loop do
        select
        when event = @events.receive?
          break unless event
          handle(event)
        when timeout(@tick)
        end
        @core.tick(Time.instant)
        flush
      end
    ensure
      @pending.each &.reply.send(false)
      @pending.clear
      @serving.set(false)
      @stopped.close
    end

    private def handle(event : Event) : Nil
      case event
      in Message
        @core.step(event, Time.instant)
      in Propose
        if index = @core.propose(event.isr, Time.instant)
          @pending << Pending.new(index, @core.term, event.reply)
        else
          event.reply.send false
        end
      in Transfer
        event.reply.send @core.transfer_leadership
      end
    end

    private def flush : Nil
      if @core.dirty?
        begin
          @storage.save(@core.hard_state)
        rescue ex
          Log.fatal(exception: ex) { "Could not persist raft state to #{@storage.path}" }
          exit 1
        end
        @core.persisted
      end
      if transport = @transport
        @core.take_outbox.each { |(to, msg)| transport.send(to, msg) }
      end
      resolve_pending
      publish_state
    end

    private def resolve_pending : Nil
      return if @pending.empty?
      @pending.reject! do |p|
        if p.term == @core.term && @core.commit_index >= p.index
          p.reply.send true
          true
        elsif p.term != @core.term || !@core.role.leader?
          p.reply.send false
          true
        else
          false
        end
      end
    end

    private def publish_state : Nil
      uri = @core.leader_uri
      isr = @core.committed_isr
      leader = @core.role.leader?
      changed = false
      @state_lock.synchronize do
        changed = uri != @leader_uri || leader != @leader
        @leader_uri = uri
        @leader = leader
        @committed_isr = isr
      end
      if changed
        Log.info { uri ? "Leader: #{uri} (term #{@core.term})" : "No leader (term #{@core.term})" }
        select
        when @leader_changed.send(nil)
        else
        end
      end
      @serving.set(@core.serving_leader?)
    end
  end
end
