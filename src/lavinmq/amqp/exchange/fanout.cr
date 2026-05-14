require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      @bindings : Sync::Shared(Set({Destination, BindingKey})) = Sync::Shared.new(Set({Destination, BindingKey}).new, :unchecked)

      def type : String
        "fanout"
      end

      def bindings_details : Array(BindingDetails)
        @bindings.shared do |b|
          b.map do |d, binding_key|
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : Destination, routing_key, arguments = nil)
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.lock(&.add?({destination, binding_key}))
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil)
        binding_key = BindingKey.new(routing_key, arguments)
        removed = false
        empty = false
        @bindings.lock do |b|
          next unless b.delete({destination, binding_key})
          removed = true
          empty = b.empty?
        end
        return false unless removed
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if @auto_delete && empty
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.shared do |b|
          b.each do |destination, _binding_key|
            yield destination
          end
        end
      end
    end
  end
end
