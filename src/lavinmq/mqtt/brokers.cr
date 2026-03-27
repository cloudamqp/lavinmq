require "./broker"
require "../clustering/replicator"
require "../vhost_store"

module LavinMQ
  module MQTT
    class Brokers
      def initialize(@vhosts : VHostStore, @replicator : Clustering::Replicator?)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @vhosts.each do |(name, vhost)|
          @brokers[name] = Broker.new(vhost, @replicator)
        end
        @vhosts.on_added { |name| @brokers[name] = Broker.new(@vhosts[name], @replicator) }
        @vhosts.on_deleted { |name| @brokers.delete(name) }
        @vhosts.on_closed { |name| @brokers[name].close }
      end

      def []?(vhost : String) : Broker?
        @brokers[vhost]?
      end
    end
  end
end
