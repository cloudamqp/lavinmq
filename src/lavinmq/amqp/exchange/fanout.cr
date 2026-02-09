require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      @bindings : Sync::Shared(Set({Destination, BindingKey})) = Sync::Shared.new(Set({Destination, BindingKey}).new)

      def type : String
        "fanout"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.shared do |bindings|
          bindings.map do |d, binding_key|
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end.each
      end

      def bind(destination : Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        added = @bindings.lock do |bindings|
          bindings.add?({destination, binding_key})
        end
        return false unless added
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil)
        binding_key = BindingKey.new(routing_key, arguments)
        result = @bindings.lock do |bindings|
          deleted = bindings.delete({destination, binding_key})
          {deleted, deleted && @auto_delete && bindings.empty?}
        end
        return false unless result[0]
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if result[1]
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.shared do |bindings|
          bindings.each do |destination, _binding_key|
            yield destination
          end
        end
      end
    end
  end
end
