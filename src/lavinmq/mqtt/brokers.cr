require "./broker"
require "../clustering/replicator"
require "../observable"
require "../vhost_store"

module LavinMQ
  module MQTT
    class Brokers
      include Observer(VHostStore::Event)

      def initialize(@vhosts : VHostStore, @replicator : Clustering::Replicator)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @vhosts.each do |(name, vhost)|
          @brokers[name] = Broker.new(vhost, @replicator)
        end
        @vhosts.register_observer(self)
      end

      def []?(vhost : String) : Broker?
        @brokers[vhost]?
      end

      def on(event : VHostStore::Event, data : Object?)
        return if data.nil?
        vhost = data.to_s
        case event
        in VHostStore::Event::Added
          @brokers[vhost] = Broker.new(@vhosts[vhost], @replicator)
        in VHostStore::Event::Deleted
          @brokers.delete(vhost)
        in VHostStore::Event::Closed
          @brokers[vhost].close
        end
      end
    end
  end
end
