require "sync/shared"
require "./exchange"

module LavinMQ
  module AMQP
    class HeadersExchange < Exchange
      @bindings : Sync::Shared(Hash(AMQP::Table, Set({Destination, BindingKey}))) = Sync::Shared.new(Hash(AMQP::Table, Set({Destination, BindingKey})).new do |h, k|
        h[k] = Set({Destination, BindingKey}).new
      end)

      def initialize(@vhost : VHost, @name : String, @durable = false,
                     @auto_delete = false, @internal = false,
                     @arguments = AMQP::Table.new)
        validate!(@arguments)
        super
      end

      def type : String
        "headers"
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

      def bind(destination : Destination, routing_key, arguments)
        validate_delayed_binding!(destination)
        validate!(arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        added = @bindings.lock do |bindings|
          bindings[arguments].add?({destination, binding_key})
        end
        return false unless added
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        result = @bindings.lock do |bindings|
          bds = bindings[arguments]
          deleted = bds.delete({destination, binding_key})
          if deleted
            bindings.delete(arguments) if bds.empty?
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

      private def validate!(arguments) : Nil
        if h = arguments
          if match = h["x-match"]?
            if match != "all" && match != "any"
              raise LavinMQ::Error::PreconditionFailed.new("x-match must be 'any' or 'all'")
            end
          end
        end
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        default_x_match = @arguments["x-match"]? || "all"
        @bindings.shared do |bindings|
          bindings.each do |args, destinations|
            if headers.nil? || headers.empty?
              next unless args.empty?
              destinations.each do |destination, _binding_key|
                yield destination
              end
            else
              x_match = args["x-match"]? || default_x_match
              is_match = case x_match
                         when "any"
                           args.any? { |k, v| !k.starts_with?("x-") && (headers.has_key?(k) && headers[k] == v) }
                         else
                           args.all? { |k, v| k.starts_with?("x-") || (headers.has_key?(k) && headers[k] == v) }
                         end
              next unless is_match
              destinations.each do |destination, _binding_key|
                yield destination
              end
            end
          end
        end
      end
    end
  end
end
