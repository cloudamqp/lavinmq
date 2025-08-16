require "./exchange"
require "sync/shared"

module LavinMQ
  module AMQP
    class FanoutExchange < Exchange
      @bindings = Sync::Shared(Set({Destination, BindingKey})).new(Set({Destination, BindingKey}).new)

      def type : String
        "fanout"
      end

      def bindings_details : Enumerable(BindingDetails)
        @bindings.read &.map do |d, binding_key|
          BindingDetails.new(name, vhost.name, binding_key, d)
        end
      end

      def bind(destination : Destination, routing_key, arguments = nil)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.write &.add?({destination, binding_key})
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.write &.delete({destination, binding_key})
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if @auto_delete && @bindings.read &.empty?
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.read &.each do |destination, _binding_key|
          yield destination
        end
      end
    end
  end
end
