require "./broker"
require "../clustering/replicator"
require "../observable"
require "../vhost_store"

module LavinMQ
  module Kafka
    class Brokers
      include Observer(VHostStore::Event)

      def initialize(@vhosts : VHostStore, @replicator : Clustering::Replicator?, @config : Config)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @vhosts.each do |(name, vhost)|
          @brokers[name] = Broker.new(vhost, @config)
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
          @brokers[vhost] = Broker.new(@vhosts[vhost], @config)
        in VHostStore::Event::Deleted
          @brokers.delete(vhost)
        in VHostStore::Event::Closed
          @brokers[vhost]?.try &.close
        end
      end
    end
  end
end
