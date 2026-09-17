require "./broker"
require "../observable"
require "../vhost_store"

module LavinMQ
  module SQS
    class Brokers
      include Observer(VHostStore::Event)

      def initialize(@vhosts : VHostStore)
        @brokers = Hash(String, Broker).new(initial_capacity: @vhosts.size)
        @closed = Atomic(Bool).new(false)
        @vhosts.each { |(name, vhost)| @brokers[name] = Broker.new(vhost) }
        @vhosts.register_observer(self)
      end

      def []?(vhost : String) : Broker?
        @brokers[vhost]?
      end

      def on(event : VHostStore::Event, data : Object?)
        return if @closed.get(:acquire)
        return if data.nil?
        vhost = data.to_s
        case event
        in VHostStore::Event::Added
          @brokers[vhost] = Broker.new(@vhosts[vhost])
        in VHostStore::Event::Deleted
          @brokers.delete(vhost).try &.close
        in VHostStore::Event::Closed
          @brokers.delete(vhost).try &.close
        end
      end

      def close
        return if @closed.swap(true)
        @vhosts.unregister_observer(self)
        @brokers.each_value &.close
        @brokers.clear
      end
    end
  end
end
