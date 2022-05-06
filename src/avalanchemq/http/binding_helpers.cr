require "../amqp"
require "../exchange"

module LavinMQ
  module HTTP
    module BindingHelpers
      private def bindings(vhost)
        vhost.exchanges.each_value.flat_map do |e|
          e.bindings_details
        end
      end

      private def binding_for_props(context, source, destination : Queue, props)
        binding = source.queue_bindings.find do |k, v|
          v.includes?(destination) && BindingDetails.hash_key(k) == props
        end
        unless binding
          not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> queue '#{destination.name}' does not exist")
        end
        binding
      end

      private def binding_for_props(context, source, destination : Exchange, props)
        binding = source.exchange_bindings.find do |k, v|
          v.includes?(destination) && BindingDetails.hash_key(k) == props
        end
        unless binding
          not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> exchange '#{destination.name}' does not exist")
        end
        binding
      end
    end
  end
end
