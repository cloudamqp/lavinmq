require "./exchange"

module LavinMQ
  module AMQP
    class DirectExchange < Exchange
      # Routing key => bindings. Both levels are replaced, never mutated, so
      # publishers on other threads route without locks.
      @bindings = CowMap(String, BindingSet).new

      def type : String
        "direct"
      end

      def bindings_details : Array(BindingDetails)
        details = Array(BindingDetails).new
        @bindings.each do |(_, bindings)|
          bindings.each do |e|
            details << BindingDetails.new(name, vhost.name, e.binding_key, e.destination)
          end
        end
        details
      end

      def binding_count : Int32
        count = 0
        @bindings.each_value { |bindings| count += bindings.size }
        count
      end

      def bind(destination : Destination, routing_key, arguments = nil) : Bool
        validate_delayed_binding!(destination)
        binding_key = BindingKey.new(routing_key, arguments)
        current = @bindings[routing_key]? || BindingSet.empty
        @bindings[routing_key] = current.add(destination, binding_key) || return false
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_bound(data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments = nil) : Bool
        binding_key = BindingKey.new(routing_key, arguments)
        current = @bindings[routing_key]? || return false
        bindings = current.delete(destination, binding_key)
        return false if bindings.same?(current)
        if bindings.empty?
          @bindings.delete routing_key
        else
          @bindings[routing_key] = bindings
        end

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        upstreams_unbound(data)

        delete if @auto_delete && @bindings.empty?
        true
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : (LavinMQ::Queue | LavinMQ::Exchange) ->)
        if bindings = @bindings[routing_key]?
          bindings.each_destination { |d| yield d }
        end
      end
    end
  end
end
