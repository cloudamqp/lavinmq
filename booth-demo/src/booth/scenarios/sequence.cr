require "random/secure"
require "./scenario"
require "../sequence_tracker"

module Booth
  # Numbered messages through a durable queue, checked for gaps on the other
  # side: the "messages lost: 0" counter. Unconfirmed messages are published
  # again after a reconnect, so duplicates are expected (at-least-once) but
  # nothing the broker confirmed may go missing.
  class Sequence < Scenario
    QUEUE        = "booth.sequence"
    WINDOW       = 1000
    LOST_GRACE   = 30.seconds
    OUTAGE_AFTER = 1.second

    getter published = Meter.new
    getter consumed = Meter.new
    getter tracker = SequenceTracker.new
    getter last_outage : Time::Span?

    @run_id = ""
    @next_seq = 0i64
    @unconfirmed = Set(Int64).new
    @retry = Deque(Int64).new
    @last_confirm : Time? = nil
    @outage_from : Time? = nil

    def initialize(@cfg : Config::Sequence, connector, totals, events)
      super(connector, totals, events)
      reset_state
    end

    def name : String
      "sequence"
    end

    def meters : Enumerable(Meter)
      {published, consumed}
    end

    def rate : Int32
      @cfg.rate
    end

    def rate=(value : Int32)
      @cfg.rate = value.clamp(0, 10_000)
    end

    protected def reset_state : Nil
      @run_id = Random::Secure.hex(4)
      @next_seq = 0i64
      @unconfirmed.clear
      @retry.clear
      @tracker = SequenceTracker.new
      @last_outage = nil
      @outage_from = nil
      @last_confirm = nil
    end

    protected def on_start(generation : Int32) : Nil
      run_id = @run_id
      amqp_worker("booth-sequence-publisher", generation) { |conn, _| publish(conn, generation, run_id) }
      amqp_worker("booth-sequence-consumer", generation) { |conn, _| consume(conn, run_id) }
      spawn(name: "booth-sequence-watch") { watch(generation) }
    end

    # Confirmed by the broker: published once and not waiting for a confirm
    # or a republish
    def confirmed?(seq : Int64) : Bool
      seq <= @next_seq && !@unconfirmed.includes?(seq) && !@retry.includes?(seq)
    end

    private def declare(ch) : Nil
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 1_000_000_i64
      ch.queue_declare(QUEUE, durable: true, args: args)
    end

    private def publish(conn, generation, run_id) : Nil
      ch = conn.channel
      declare(ch)
      ch.confirm_select
      # Published on this channel and not yet confirmed. Kept here rather
      # than trusting the client library to call back for every publish
      # once the connection is gone.
      pending = Set(Int64).new
      pacer = Pacer.new(@cfg.rate)
      while active?(generation)
        pacer.rate = @cfg.rate
        due = pacer.due
        # Whatever wasn't confirmed before a failover goes first
        due = Math.max(due, @retry.size.clamp(0, WINDOW))
        due.times do
          while pending.size >= WINDOW
            raise IO::Error.new("Connection closed") if ch.closed?
            sleep 5.milliseconds
          end
          # A publish on a channel the client already tore down is
          # silently dropped and never confirmed
          raise IO::Error.new("Connection closed") if ch.closed?
          seq = @retry.shift? || (@next_seq += 1)
          pending << seq
          @unconfirmed << seq
          headers = AMQ::Protocol::Table.new
          headers["seq"] = seq
          headers["run"] = run_id
          props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8, headers: headers)
          ch.basic_publish(seq.to_s, "", QUEUE, props: props) do |ok|
            confirm(seq, ok, generation, pending)
          end
        end
      end
    ensure
      if pending && active?(generation)
        # Unknown whether these reached the queue, so they go again:
        # at-least-once, a duplicate rather than a gap
        pending.each { |s| @unconfirmed.delete(s) }
        @retry = Deque(Int64).new((@retry.to_a + pending.to_a).sort!)
        pending.clear
      end
    end

    private def confirm(seq : Int64, ok : Bool, generation : Int32, pending : Set(Int64)) : Nil
      return unless active?(generation)
      return unless pending.delete(seq) # already handed to the retry queue
      @unconfirmed.delete(seq)
      unless ok # nacked, or the connection went before the confirm
        @retry << seq
        return
      end
      @published.mark
      @totals.published.mark
      now = Time.utc
      @last_confirm = now
      if from = @outage_from
        @outage_from = nil
        took = now - from
        @last_outage = took
        @events.outage(from, now)
        @tracker.check(LOST_GRACE) { |s| confirmed?(s) }
        @events.add("recovered", "Confirms flowing again after #{took.total_seconds.round(1)} s, " \
                                 "#{@tracker.lost} messages lost")
      end
    end

    private def consume(conn, run_id) : Nil
      ch = conn.channel
      declare(ch)
      ch.basic_qos(500)
      ch.basic_consume(QUEUE, no_ack: false, block: true) do |msg|
        headers = msg.properties.headers
        if headers && headers["run"]? == run_id && (seq = headers["seq"]?.as?(Int64))
          @tracker.receive(seq)
          @consumed.mark
          @totals.delivered.mark
        end
        msg.ack
      end
    end

    # Notices when confirms stop arriving, and settles gaps into lost
    private def watch(generation) : Nil
      while active?(generation)
        sleep 250.milliseconds
        if (last = @last_confirm) && @outage_from.nil? && @cfg.rate > 0 &&
           Time.utc - last > OUTAGE_AFTER
          @outage_from = last
        end
        @tracker.check(LOST_GRACE) { |s| confirmed?(s) }
      end
    end

    protected def snapshot_fields(json : JSON::Builder) : Nil
      json.field "published_rate", published.rate
      json.field "consumed_rate", consumed.rate
      json.field "published", published.count
      json.field "received", tracker.received
      json.field "highest", tracker.highest
      json.field "in_flight", @unconfirmed.size + @retry.size
      json.field "missing", tracker.missing
      json.field "duplicates", tracker.duplicates
      json.field "lost", tracker.lost
      json.field "outage", !@outage_from.nil?
      json.field "outage_since", @outage_from.try &.to_unix_ms
      json.field "last_outage", last_outage.try &.total_seconds
    end
  end
end
