require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    class DirectExchange < Exchange
      @bindings : Sync::Shared(Hash(String, Set({Destination, BindingKey}))) = Sync::Shared.new(
        Hash(String, Set({Destination, BindingKey})).new { |h, k| h[k] = Set({Destination, BindingKey}).new },
        :unchecked)

      def type : String
        "direct"
      end

      def bindings_details : Array(BindingDetails)
        @bindings.shared do |b|
          b.flat_map do |_key, ds|
            ds.map do |d, binding_key|
              BindingDetails.new(name, vhost.name, binding_key, d)
            end
          end
        end
      end

      def bind(destination : Destination, routing_key, arguments = nil) : Bool
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        return false unless @bindings.lock { |b| b[routing_key].add?({destination, binding_key}) }
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        removed = false
        all_empty = false
        @bindings.lock do |b|
          rk_bindings = b[routing_key]?
          next unless rk_bindings
          next unless rk_bindings.delete({destination, binding_key})
          removed = true
          b.delete routing_key if rk_bindings.empty?
          all_empty = b.each_value.all?(&.empty?)
        end
        return false unless removed
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)
        delete if @auto_delete && all_empty
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.shared do |b|
          b[routing_key]?.try &.each do |destination, _arguments|
            yield destination
          end
        end
      end
    end
  end
end
