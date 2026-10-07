require "./broker"
require "../vhost_store"

module LavinMQ
  module MQTT
    class Brokers
      def initialize(@vhosts : VHostStore)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @closed = Atomic(Bool).new(false)
        populate
        @vhosts.mqtt_brokers = self
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

      def create(vhost : VHost) : Nil
        return if @closed.get(:acquire)
        @brokers[vhost.name] = Broker.new(vhost)
      end

      def delete(vhost : String) : Nil
        return if @closed.get(:acquire)
        @brokers.delete(vhost)
      end

      def close(vhost : String) : Nil
        return if @closed.get(:acquire)
        @brokers.delete(vhost).try &.close
      end

      def close
        return if @closed.swap(true)
        @vhosts.mqtt_brokers = nil if @vhosts.mqtt_brokers == self
        close_brokers
      end

      private def close_brokers
        @brokers.each_value &.close
        @brokers.clear
      end
    end
  end
end
