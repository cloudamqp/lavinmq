require "./broker"
require "../observable"
require "../vhost_store"
require "../auth/permission_group_store"

module LavinMQ
  module MQTT
    class Brokers
      include Observer(VHostStore::Event)

      def initialize(@vhosts : VHostStore, @permission_groups : Auth::PermissionGroupStore)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @closed = Atomic(Bool).new(false)
        populate
        @vhosts.register_observer(self)
      end

      private def populate
        @vhosts.each do |(name, vhost)|
          @brokers[name] = Broker.new(vhost, @permission_groups)
        end
      end

      def []?(vhost : String) : Broker?
        @brokers[vhost]?
      end

      def broker(vhost : String) : Broker
        @brokers[vhost]
      end

      def on(event : VHostStore::Event, data : Object?)
        return if @closed.get(:acquire)
        return if data.nil?
        vhost = data.to_s
        case event
        in VHostStore::Event::Added
          @brokers[vhost] = Broker.new(@vhosts[vhost], @permission_groups)
        in VHostStore::Event::Deleted
          @brokers.delete(vhost)
        in VHostStore::Event::Closed
          @brokers.delete(vhost).try &.close
        end
      end

      def close
        return if @closed.swap(true)
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
