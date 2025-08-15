require "./exchange"
require "sync/shared"

module LavinMQ
  module AMQP
    class HeadersExchange < Exchange
      @bindings = Sync::Shared(Hash(AMQP::Table, Set({Destination, BindingKey}))).new(
        Hash(AMQP::Table, Set({Destination, BindingKey})).new do |h, k|
          h[k] = Set({Destination, BindingKey}).new
        end
      )

      def initialize(@vhost : VHost, @name : String, @durable = false,
                     @auto_delete = false, @internal = false,
                     @arguments = AMQP::Table.new)
        validate!(@arguments)
        super
      end

      def type : String
        "headers"
      end

      def bindings_details : Enumerable(BindingDetails)
        @bindings.read &.flat_map do |_args, ds|
          ds.each.map do |d, binding_key|
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : Destination, routing_key, arguments)
        validate!(arguments)
        arguments ||= AMQP::Table.new
        binding_key = BindingKey.new(routing_key, arguments)
        added = @bindings.write do |bindings|
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
        result = @bindings.write do |bindings|
          bds = bindings[arguments]
          deleted = bds.delete({destination, binding_key})
          bindings.delete(arguments) if bds.empty?
          {deleted, bindings.each_value.all?(&.empty?)}
        end
        deleted, all_empty = result
        return false unless deleted

        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && all_empty
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
        @bindings.read &.each do |args, destinations|
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
