require "./scenario"

module Booth
  # Competing consumers on a durable queue, publishers waiting for confirms
  class WorkQueue < Scenario
    QUEUE  = "booth.work"
    WINDOW = 500 # unconfirmed publishes per publisher

    getter published = Meter.new
    getter consumed = Meter.new
    property queue_depth = 0i64

    def initialize(@cfg : Config::WorkQueue, connector, totals, events)
      super(connector, totals, events)
    end

    def name : String
      "work_queue"
    end

    def meters : Enumerable(Meter)
      {published, consumed}
    end

    def rate : Int32
      @cfg.rate
    end

    def rate=(value : Int32)
      @cfg.rate = value.clamp(0, 100_000)
    end

    protected def on_start(generation : Int32) : Nil
      @cfg.publishers.times do |i|
        amqp_worker("booth-work-publisher-#{i}", generation) { |conn, _| publish(conn, generation) }
      end
      @cfg.consumers.times do |i|
        amqp_worker("booth-work-consumer-#{i}", generation) { |conn, _| consume(conn) }
      end
    end

    protected def reset_state : Nil
    end

    private def declare(ch) : Nil
      args = AMQP::Client::Arguments.new
      args["x-max-length"] = 100_000_i64
      ch.queue_declare(QUEUE, durable: true, args: args)
    end

    private def per_publisher_rate
      (@cfg.rate / @cfg.publishers).ceil.to_i
    end

    private def publish(conn, generation) : Nil
      ch = conn.channel
      declare(ch)
      ch.confirm_select
      body = Bytes.new(@cfg.size, 'x'.ord.to_u8)
      props = AMQ::Protocol::Properties.new(delivery_mode: 2_u8)
      pacer = Pacer.new(per_publisher_rate)
      outstanding = 0
      while active?(generation)
        pacer.rate = per_publisher_rate
        pacer.due.times do
          while outstanding >= WINDOW
            raise IO::Error.new("Connection closed") if ch.closed?
            sleep 5.milliseconds
          end
          # A publish on a channel the client already tore down is
          # silently dropped and never confirmed
          raise IO::Error.new("Connection closed") if ch.closed?
          outstanding += 1
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

    private def consume(conn) : Nil
      ch = conn.channel
      declare(ch)
      ch.basic_qos(200)
      ch.basic_consume(QUEUE, no_ack: false, block: true) do |msg|
        @consumed.mark
        @totals.delivered.mark
        msg.ack
      end
    end

    protected def snapshot_fields(json : JSON::Builder) : Nil
      json.field "published_rate", published.rate
      json.field "consumed_rate", consumed.rate
      json.field "published", published.count
      json.field "consumed", consumed.count
      json.field "queue_depth", queue_depth
      json.field "publishers", @cfg.publishers
      json.field "consumers", @cfg.consumers
    end
  end
end
