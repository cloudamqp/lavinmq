require "./exchange"

module LavinMQ
  module AMQP
    class DirectExchange < Exchange
      @bindings = Hash(String, Set(Destination)).new do |h, k|
        h[k] = Set(Destination).new
      end

      def type : String
        "direct"
      end

      def bindings_details : Iterator(BindingDetails)
        @bindings.each.flat_map do |key, ds|
          ds.each.map do |d|
            binding_key = BindingKey.new(key)
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : Destination, routing_key, headers = nil) : Bool
        return false unless @bindings[routing_key].add? destination
        binding_key = BindingKey.new(routing_key)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, headers = nil) : Bool
        rk_bindings = @bindings[routing_key]
        return false unless rk_bindings.delete destination
        @bindings.delete routing_key if rk_bindings.empty?

        binding_key = BindingKey.new(routing_key)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        @bindings[routing_key].each do |destination|
          yield destination
        end
      end
    end
  end
end
