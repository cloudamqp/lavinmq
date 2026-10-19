require "./log"
require "./config"
require "./events"
require "./stats"
require "./cluster_monitor"
require "./mqtt_client"
require "./connector"
require "./scenarios/scenario"
require "./scenarios/work_queue"
require "./scenarios/sequence"
require "./scenarios/stream"
require "./scenarios/iot"

module Booth
  class App
    SNAPSHOT_INTERVAL = 250.milliseconds

    getter config : Config
    getter events = Events.new
    getter monitor : ClusterMonitor
    getter connector : Connector
    getter totals = Totals.new
    getter work_queue : WorkQueue
    getter sequence : Sequence
    getter stream : Stream
    getter iot : Iot
    getter scenarios : Array(Scenario)
    # Prebuilt once per interval and shared by every dashboard connection
    getter snapshot = "{}"

    def initialize(@config)
      @monitor = ClusterMonitor.new(@config, @events)
      @connector = Connector.new(@config, @monitor)
      s = @config.scenarios
      @work_queue = WorkQueue.new(s.work_queue, @connector, @totals, @events)
      @sequence = Sequence.new(s.sequence, @connector, @totals, @events)
      @stream = Stream.new(s.stream, @connector, @totals, @events)
      @iot = Iot.new(s.iot, @connector, @totals, @events, @config.nodes)
      @scenarios = [@work_queue, @sequence, @stream, @iot] of Scenario
    end

    def scenario(name : String) : Scenario?
      @scenarios.find { |s| s.name == name }
    end

    def start : Nil
      @monitor.poll # know who leads before the first connections
      spawn(name: "cluster monitor") { @monitor.run }
      spawn(name: "meter ticker") { tick_loop }
      spawn(name: "snapshot builder") { snapshot_loop }
      s = @config.scenarios
      @work_queue.start if s.work_queue.enabled?
      @sequence.start if s.sequence.enabled?
      @stream.start if s.stream.enabled?
      @iot.start if s.iot.enabled?
    end

    def reset : Nil
      @scenarios.each &.reset
      @totals.meters.each &.reset
      @events.clear
      @events.add("info", "Demo reset")
    end

    private def tick_loop : Nil
      loop do
        sleep 1.second
        now = Time.instant
        @scenarios.each &.tick(now)
        @totals.meters.each &.tick(now)
        @work_queue.queue_depth = @monitor.queues[WorkQueue::QUEUE]? || 0i64
      end
    end

    private def snapshot_loop : Nil
      loop do
        @monitor.nodes.each { |n| n.clients = @connector.clients_per_host[n.host] }
        @snapshot = build_snapshot
        sleep SNAPSHOT_INTERVAL
      end
    end

    def build_snapshot : String
      JSON.build do |json|
        json.object do
          json.field "title", @config.title
          json.field "now", Time.utc.to_unix_ms
          json.field "backend", @monitor.backend
          json.field "term", @monitor.term
          json.field "leader", @monitor.leader.try &.name
          json.field "leaderless_since", @monitor.leaderless_since.try &.to_unix_ms
          json.field "nodes", @monitor.nodes
          json.field "overview" do
            overview_json(json)
          end
          json.field "throughput" do
            json.object do
              json.field "published", @totals.published.history
              json.field "delivered", @totals.delivered.history
            end
          end
          json.field "scenarios" do
            json.object do
              @scenarios.each { |s| json.field s.name, s }
            end
          end
          @events.to_json_fields(json)
        end
      end
    end

    private def overview_json(json) : Nil
      o = @monitor.overview
      json.object do
        json.field "connections", o.dig?("object_totals", "connections")
        json.field "queues", o.dig?("object_totals", "queues")
        json.field "messages", o.dig?("queue_totals", "messages")
        json.field "publish_rate", o.dig?("message_stats", "publish_details", "rate")
        json.field "deliver_rate", o.dig?("message_stats", "deliver_details", "rate")
        json.field "version", o["lavinmq_version"]?
      end
    end
  end
end
