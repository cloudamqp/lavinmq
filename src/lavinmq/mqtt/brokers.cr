require "./broker"
require "../observable"
require "../vhost_store"

module LavinMQ
  module MQTT
    class Brokers
      include Observer(VHostStore::Event)

      def initialize(@vhosts : VHostStore)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @closed = false
        populate
        @vhosts.register_observer(self)
      end

      private def populate
        @vhosts.each do |(name, vhost)|
          @brokers[name] = Broker.new(vhost)
        end
      end

      def []?(vhost : String) : Broker?
        @brokers[vhost]?
      end

      def broker(vhost : String) : Broker
        @brokers[vhost]
      end

      def on(event : VHostStore::Event, data : Object?)
        return if @closed
        return if data.nil?
        vhost = data.to_s
        case event
        in VHostStore::Event::Added
          @brokers[vhost] = Broker.new(@vhosts[vhost])
        in VHostStore::Event::Deleted
          @brokers.delete(vhost)
        in VHostStore::Event::Closed
          @brokers.delete(vhost).try &.close
        end
      end

      def close
        return if @closed
        @closed = true
        @vhosts.unregister_observer(self)
        close_brokers
      end

      private def close_brokers
        @brokers.each_value &.close
        @brokers.clear
      end
    end
  end
end
