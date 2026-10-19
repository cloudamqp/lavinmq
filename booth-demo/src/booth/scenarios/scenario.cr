require "json"
require "../connector"
require "../events"
require "../stats"

module Booth
  # Messages through the cluster across all scenarios, for the hero chart
  class Totals
    getter published = Meter.new
    getter delivered = Meter.new

    def meters
      {published, delivered}
    end
  end

  abstract class Scenario
    alias Closable = AMQP::Client::Connection | MqttClient

    getter? running = false
    getter reconnects = 0
    # Bumped on every stop, so workers of an earlier run know to quit
    @generation = 0
    @open = Set(Closable).new

    def initialize(@connector : Connector, @totals : Totals, @events : Events)
    end

    abstract def name : String
    abstract def meters : Enumerable(Meter)
    abstract def rate : Int32
    abstract def rate=(value : Int32)
    protected abstract def on_start(generation : Int32) : Nil
    protected abstract def reset_state : Nil
    protected abstract def snapshot_fields(json : JSON::Builder) : Nil

    def start : Nil
      return if @running
      @running = true
      on_start(@generation)
    end

    def stop : Nil
      return unless @running
      @running = false
      @generation += 1
      @open.each { |c| c.close rescue nil }
      @open.clear
    end

    def reset : Nil
      was_running = running?
      stop
      reset_state
      meters.each &.reset
      @reconnects = 0
      start if was_running
    end

    def tick(now) : Nil
      meters.each &.tick(now)
    end

    protected def active?(generation) : Bool
      @running && generation == @generation
    end

    # Runs the block on a fresh AMQP connection, over and over, until stopped
    protected def amqp_worker(worker : String, generation : Int32, &block : AMQP::Client::Connection, String -> Nil) : Nil
      spawn(name: worker) do
        while active?(generation)
          @connector.with_amqp(worker) do |conn, host|
            next unless active?(generation)
            @open << conn
            begin
              block.call(conn, host)
            rescue ex
              Log.debug { "#{worker}: #{ex.message}" }
            ensure
              @open.delete(conn)
            end
            # A block that returns on its own with the connection still up
            # (e.g. a stream reader starting over) didn't lose it
            if active?(generation) && conn.closed?
              @reconnects += 1
              sleep 100.milliseconds
            end
          end
        end
      end
    end

    protected def track(c : Closable, &)
      @open << c
      begin
        yield
      ensure
        @open.delete(c)
      end
    end

    def to_json(json : JSON::Builder)
      json.object do
        json.field "running", running?
        json.field "rate", rate
        json.field "reconnects", reconnects
        snapshot_fields(json)
      end
    end
  end
end
