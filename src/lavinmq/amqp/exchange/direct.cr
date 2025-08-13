require "./exchange"
require "sync/shared"

module LavinMQ
  module AMQP
    class DirectExchange < Exchange
      @bindings = Sync::Shared(Hash(String, Set({Destination, BindingKey}))).new(
        Hash(String, Set({Destination, BindingKey})).new do |h, k|
          h[k] = Set({Destination, BindingKey}).new
        end
      )

      def type : String
        "direct"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.read do |bindings|
          bindings.each.flat_map do |_key, ds|
            ds.each.map do |d, binding_key|
              BindingDetails.new(name, vhost.name, binding_key, d)
            end
          end
        end
      end

      def bind(destination : Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        added = @bindings.write do |bindings|
          bindings[routing_key].add?({destination, binding_key})
        end
        return false unless added
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        result = @bindings.write do |bindings|
          rk_bindings = bindings[routing_key]
          deleted = rk_bindings.delete({destination, binding_key})
          bindings.delete routing_key if rk_bindings.empty?
          {deleted, bindings.each_value.all?(&.empty?)}
        end
        deleted, all_empty = result
        return false unless deleted

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && all_empty
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings.read do |bindings|
          bindings[routing_key].each do |destination, _arguments|
            yield destination
          end
        end
      end
    end
  end
end
