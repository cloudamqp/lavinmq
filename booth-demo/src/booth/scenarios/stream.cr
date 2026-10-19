require "./scenario"

module Booth
  # One producer appending to a stream, and readers that each keep their own
  # position: one following the head, one replaying from the start, one that
  # started five minutes back. After a failover they continue from the
  # offset they had, rather than starting over.
  class Stream < Scenario
    QUEUE  = "booth.stream"
    WINDOW = 1000
    # The replay reader starts over this long after it has caught up, so
    # there is always something replaying on screen
    REWIND_AFTER = 60.seconds

    class Rewind < Exception; end

    class Reader
      getter name : String
      getter label : String
      getter meter = Meter.new
      property offset : Int64?
      property resumes = 0
      property caught_up_since : Time::Instant?
      property? rewind = false
      getter? rate_limited : Bool

      def initialize(@name, @label, @rate_limited, &@initial_offset : -> AMQ::Protocol::Field)
      end

      def initial_offset : AMQ::Protocol::Field
        @initial_offset.call
      end
    end

    getter published = Meter.new
    getter readers : Array(Reader)

    def initialize(@cfg : Config::Stream, connector, totals, events)
      super(connector, totals, events)
      @readers = [
        Reader.new("live", "Live", false) { "next".as(AMQ::Protocol::Field) },
        Reader.new("replay", "Replay from start", true) { "first".as(AMQ::Protocol::Field) },
        # Rate limited too, or catching up on five minutes at full speed
        # dwarfs everything else on the throughput chart
        Reader.new("recent", "Last 5 minutes", true) { (Time.utc - 5.minutes).as(AMQ::Protocol::Field) },
      ]
    end

    def name : String
      "stream"
    end

    def meters : Enumerable(Meter)
      [published] + @readers.map(&.meter)
    end

    def rate : Int32
      @cfg.rate
    end

    def rate=(value : Int32)
      @cfg.rate = value.clamp(0, 50_000)
    end

    def replay_rate=(value : Int32)
      @cfg.replay_rate = value.clamp(1, 50_000)
    end

    protected def reset_state : Nil
      @readers.each do |r|
        r.offset = nil
        r.resumes = 0
        r.caught_up_since = nil
      end
    end

    protected def on_start(generation : Int32) : Nil
      amqp_worker("booth-stream-producer", generation) { |conn, _| produce(conn, generation) }
      @readers.each do |r|
        amqp_worker("booth-stream-#{r.name}", generation) { |conn, _| read(conn, r) }
      end
      spawn(name: "booth-stream-rewind") { watch_replay(generation) }
    end

    private def declare(ch) : Nil
      args = AMQP::Client::Arguments.new
      args["x-queue-type"] = "stream"
      args["x-max-length-bytes"] = 256_i64 * 1024 * 1024
      ch.queue_declare(QUEUE, durable: true, args: args)
    end

    private def produce(conn, generation) : Nil
      ch = conn.channel
      declare(ch)
      ch.confirm_select
      pacer = Pacer.new(@cfg.rate)
      outstanding = 0
      n = 0i64
      while active?(generation)
        pacer.rate = @cfg.rate
        pacer.due.times do
          while outstanding >= WINDOW
            raise IO::Error.new("Connection closed") if ch.closed?
            sleep 5.milliseconds
          end
          # A publish on a channel the client already tore down is
          # silently dropped and never confirmed
          raise IO::Error.new("Connection closed") if ch.closed?
          outstanding += 1
          props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8, timestamp: Time.utc)
          body = %({"reading":#{n += 1},"value":#{(Math.sin(n / 50) * 10 + 20).round(2)}})
          ch.basic_publish(body, "", QUEUE, props: props) do |ok|
            outstanding -= 1
            if ok
              @published.mark
              @totals.published.mark
            end
          end
        end
      end
    end

    private def read(conn, r : Reader) : Nil
      ch = conn.channel
      declare(ch)
      ch.basic_qos(r.rate_limited? ? 50 : 500)
      args = AMQP::Client::Arguments.new
      if offset = r.offset
        args["x-stream-offset"] = offset + 1
        r.resumes += 1
      else
        args["x-stream-offset"] = r.initial_offset
      end
      pacer = Pacer.new(@cfg.replay_rate) if r.rate_limited?
      budget = 0
      ch.basic_consume(QUEUE, tag: "booth-#{r.name}", no_ack: false, block: true, args: args) do |msg|
        raise Rewind.new if r.rewind?
        if off = msg.properties.headers.try &.["x-stream-offset"]?
          r.offset = off.as?(Int64) || off.as?(Int32).try(&.to_i64)
        end
        r.meter.mark
        @totals.delivered.mark
        if pacer
          pacer.rate = @cfg.replay_rate
          budget = pacer.due if budget <= 0
          budget -= 1
        end
        msg.ack
      end
    rescue Rewind
      r.rewind = false
      r.offset = nil
      r.caught_up_since = nil
    end

    def head : Int64?
      @readers.compact_map(&.offset).max?
    end

    def lag(r : Reader) : Int64?
      if (h = head) && (o = r.offset)
        h - o
      end
    end

    private def watch_replay(generation) : Nil
      replay = @readers.find! { |r| r.name == "replay" }
      while active?(generation)
        sleep 1.second
        lag = lag(replay)
        if lag && lag < @cfg.rate
          since = replay.caught_up_since ||= Time.instant
          if Time.instant - since > REWIND_AFTER
            replay.rewind = true
          end
        else
          replay.caught_up_since = nil
        end
      end
    end

    protected def snapshot_fields(json : JSON::Builder) : Nil
      json.field "published_rate", published.rate
      json.field "head", head
      json.field "replay_rate", @cfg.replay_rate
      json.field "readers" do
        json.array do
          @readers.each do |r|
            json.object do
              json.field "name", r.name
              json.field "label", r.label
              json.field "offset", r.offset
              json.field "rate", r.meter.rate
              json.field "lag", lag(r)
              json.field "resumes", r.resumes
            end
          end
        end
      end
    end
  end
end
