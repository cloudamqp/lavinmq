require "./binding_key"
require "./sortable_json"

module LavinMQ
  struct BindingDetails
    include SortableJSON
    getter source, vhost, binding_key, destination

    def initialize(@source : String, @vhost : String,
                   @binding_key : BindingKey, @destination : Destination)
    end

    def arguments
      @binding_key.arguments
    end

    def routing_key
      @binding_key.routing_key
    end

    def details_tuple
      {
        source:           @source,
        vhost:            @vhost,
        destination:      @destination.name,
        destination_type: @destination.is_a?(Queue) ? "queue" : "exchange",
        routing_key:      @binding_key.routing_key,
        arguments:        @binding_key.arguments,
        properties_key:   @binding_key.properties_key,
      }
    end

    def search_match?(value : String) : Bool
      @destination.name.includes?(value) ||
        @binding_key.routing_key.includes?(value)
    end

    def search_match?(value : Regex) : Bool
      value === @destination.name ||
        value === @binding_key.routing_key
    end
  end
end
