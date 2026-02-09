require "sync/shared"
require "./broker"
require "../clustering/replicator"
require "../observable"
require "../vhost_store"

module LavinMQ
  module MQTT
    class Brokers
      include Observer(VHostStore::Event)
      @brokers : Sync::Shared(Hash(String, Broker))

      def initialize(@vhosts : VHostStore, @replicator : Clustering::Replicator?)
        @brokers = Sync::Shared.new(Hash(String, Broker).new(initial_capacity: @vhosts.size))
        @vhosts.each do |(name, vhost)|
          @brokers.lock { |brokers| brokers[name] = Broker.new(vhost, @replicator) }
        end
        @vhosts.register_observer(self)
      end

      def []?(vhost : String) : Broker?
        @brokers.shared { |brokers| brokers[vhost]? }
      end

      def on(event : VHostStore::Event, data : Object?)
        return if data.nil?
        vhost = data.to_s
        case event
        in VHostStore::Event::Added
          broker = Broker.new(@vhosts[vhost], @replicator)
          @brokers.lock { |brokers| brokers[vhost] = broker }
        in VHostStore::Event::Deleted
          @brokers.lock(&.delete(vhost))
        in VHostStore::Event::Closed
          broker = @brokers.shared { |brokers| brokers[vhost]? }
          broker.try &.close
        end
      end
    end
  end
end
