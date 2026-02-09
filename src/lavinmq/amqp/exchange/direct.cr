require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    class DirectExchange < Exchange
      @bindings : Sync::Shared(Hash(String, Set({Destination, BindingKey}))) = Sync::Shared.new(Hash(String, Set({Destination, BindingKey})).new do |h, k|
        h[k] = Set({Destination, BindingKey}).new
      end)

      def type : String
        "direct"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.shared do |bindings|
          bindings.each_value.flat_map do |ds|
            ds.map do |d, binding_key|
              BindingDetails.new(name, vhost.name, binding_key, d)
            end
          end.to_a
        end.each
      end

      def bind(destination : Destination, routing_key, arguments = nil) : Bool
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        added = @bindings.lock do |bindings|
          bindings[routing_key].add?({destination, binding_key})
        end
        return false unless added
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        result = @bindings.lock do |bindings|
          rk_bindings = bindings[routing_key]
          deleted = rk_bindings.delete({destination, binding_key})
          if deleted
            bindings.delete routing_key if rk_bindings.empty?
            {true, @auto_delete && bindings.each_value.all?(&.empty?)}
          else
            {false, false}
          end
        end
        return false unless result[0]

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if result[1]
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.shared do |bindings|
          if set = bindings[routing_key]?
            set.each do |destination, _arguments|
              yield destination
            end
          end
        end
      end
    end
  end
end
