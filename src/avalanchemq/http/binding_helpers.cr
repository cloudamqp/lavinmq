require "../amqp"
require "../exchange"

module AvalancheMQ
  module HTTP
    module BindingHelpers
      alias BindingDetails = NamedTuple(
        source: String,
        vhost: String,
        destination: String,
        destination_type: String,
        routing_key: String,
        arguments: Hash(String, AMQP::Field)?,
        properties_key: String)

      private def bindings(vhost)
        vhost.exchanges.each_value.flat_map do |e|
          e.bindings_details
        end.each.map { |b| map_binding(b) }
      end

      private def map_binding(b)
        key_tuple = {b[:routing_key].as(String), b[:arguments].as?(Hash(String, AMQP::Field))}
        b.merge({properties_key: hash_key(key_tuple)})
      end

      private def binding_for_props(context, source, destination, props)
        binding = source.bindings.find do |k, v|
          v.includes?(destination) && hash_key(k) == props
        end
        unless binding
          type = destination.is_a?(Queue) ? "queue" : "exchange"
          not_found(context, "Binding '#{props}' on exchange '#{source.name}' -> #{type} '#{destination.name}' does not exist")
        end
        binding
      end

      private def hash_key(key : Exchange::BindingKey)
        if key[1].nil? || key[1].try &.empty?
          key[0]
        else
          hsh = Base64.urlsafe_encode(key[1].to_s)
          "#{key[0]}~#{hsh}"
        end
      end

      private def unbind_prop(source : Queue | Exchange, destination : Queue | Exchange, key : String)
        key = source.bindings.keys.find do |k|
          hash_key(k) == key
        end
        source.unbind(destination, key[0], key[1]) if key
      end
    end
  end
end
