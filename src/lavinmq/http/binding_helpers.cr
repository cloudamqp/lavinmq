require "../amqp"
require "../exchange"

module LavinMQ
  module HTTP
    module BindingHelpers
      private def bindings(vhost)
        bindings = Array(BindingDetails).new
        vhost.each_exchange do |e|
          bindings.concat(e.bindings_details)
        end
        bindings
      end

      private def binding_for_props(context, source, destination : Destination, props)
        binding = source.bindings_details.find do |bd|
          bd.destination == destination && bd.binding_key.properties_key == props
        end
        unless binding
          not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> exchange '#{destination.name}' does not exist")
        end
        binding
      end
    end
  end
end
