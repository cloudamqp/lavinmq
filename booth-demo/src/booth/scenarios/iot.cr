require "./scenario"

module Booth
  # Many small MQTT devices, each on its own connection, spread over all the
  # nodes. A dashboard client subscribes to all their readings. The UI draws
  # a dot per device colored by the node it's connected to, so pulling a
  # follower dims a third of them, pulling the leader all of them.
  class Iot < Scenario
    TOPIC_FILTER = "sensors/+/temp"
    # A device counts as "seen" if a reading arrived within this many intervals
    SEEN_WITHIN = 2.5

    getter sent = Meter.new
    getter received = Meter.new
    @device_node : Array(Int32)
    @last_seen : Array(Time::Instant?)
    @node_index : Hash(String, Int32)

    def initialize(@cfg : Config::Iot, connector, totals, events, nodes : Array(Config::Node))
      super(connector, totals, events)
      @device_node = Array.new(@cfg.max_devices, -1)
      @last_seen = Array(Time::Instant?).new(@cfg.max_devices, nil)
      @node_index = nodes.each_with_index.to_h { |n, i| {n.host, i} }
      @cfg.devices = @cfg.devices.clamp(0, @cfg.max_devices)
    end

    def name : String
      "iot"
    end

    def meters : Enumerable(Meter)
      {sent, received}
    end

    # For this scenario the knob is the number of devices
    def rate : Int32
      @cfg.devices
    end

    def rate=(value : Int32)
      old = @cfg.devices
      @cfg.devices = value.clamp(0, @cfg.max_devices)
      if running?
        (old...@cfg.devices).each { |i| spawn_device(i, @generation) }
      end
    end

    protected def reset_state : Nil
      @last_seen.fill(nil)
    end

    protected def on_start(generation : Int32) : Nil
      spawn(name: "booth-iot-dashboard") { dashboard(generation) }
      @cfg.devices.times { |i| spawn_device(i, generation) }
    end

    private def spawn_device(i, generation)
      spawn(name: "booth-iot-device-#{i}") { device(i, generation) }
    end

    private def device_active?(i, generation)
      active?(generation) && i < @cfg.devices
    end

    private def device(i : Int32, generation : Int32) : Nil
      interval = @cfg.interval
      sleep Random.rand(interval).seconds # don't all report at once
      temp = 18.0 + Random.rand(6.0)
      while device_active?(i, generation)
        host = @connector.next_host
        begin
          c = client = @connector.connect_mqtt(host, "booth-device-#{i}")
          track(c) do
            @connector.track(host) do
              @device_node[i] = @node_index[host]
              while device_active?(i, generation)
                temp = (temp + Random.rand(-0.2..0.2)).clamp(10.0, 35.0)
                c.publish("sensors/#{i}/temp", %({"id":#{i},"celsius":#{temp.round(2)}}).to_slice)
                c.ping # flushes, and notices a server that's gone
                @sent.mark
                @totals.published.mark
                sleep (interval * Random.rand(0.8..1.2)).seconds
              end
            end
          end
        rescue ex
          Log.debug { "device #{i} at #{host}: #{ex.message}" }
          if device_active?(i, generation)
            @reconnects += 1
            sleep Random.rand(0.2..1.0).seconds # spread out the reconnect storm
          end
        ensure
          @device_node[i] = -1
          client.try &.close
          client = nil
        end
      end
    end

    # The subscriber the dashboard's dots come from
    private def dashboard(generation : Int32) : Nil
      while active?(generation)
        host = @connector.next_host
        begin
          c = client = @connector.connect_mqtt(host, "booth-dashboard")
          c.subscribe(TOPIC_FILTER)
          track(c) do
            spawn(name: "booth-iot-dashboard-ping") do
              until c.closed?
                sleep 1.second
                c.send_ping rescue break
              end
            end
            while active?(generation)
              packet = c.read_packet
              if packet.is_a?(MQTT::Protocol::Publish)
                if id = packet.topic.split('/')[1]?.try(&.to_i?)
                  @last_seen[id] = Time.instant if 0 <= id < @last_seen.size
                end
                @received.mark
                @totals.delivered.mark
              end
            end
          end
        rescue ex
          Log.debug { "dashboard subscriber at #{host}: #{ex.message}" }
          sleep 200.milliseconds if active?(generation)
        ensure
          client.try &.close
          client = nil
        end
      end
    end

    def connected : Int32
      @device_node.first(@cfg.devices).count(&.>=(0))
    end

    # One char per device: 'a'+node when connected and recently seen by the
    # dashboard, 'A'+node when connected but not seen, '0' when disconnected
    def dots : String
      now = Time.instant
      seen_within = (@cfg.interval * SEEN_WITHIN).seconds
      String.build(@cfg.devices) do |s|
        @cfg.devices.times do |i|
          node = @device_node[i]
          if node < 0
            s << '0'
          elsif (seen = @last_seen[i]) && now - seen < seen_within
            s << ('a' + node)
          else
            s << ('A' + node)
          end
        end
      end
    end

    protected def snapshot_fields(json : JSON::Builder) : Nil
      json.field "devices", @cfg.devices
      json.field "max_devices", @cfg.max_devices
      json.field "connected", connected
      json.field "sent_rate", sent.rate
      json.field "received_rate", received.rate
      json.field "dots", dots
    end
  end
end
