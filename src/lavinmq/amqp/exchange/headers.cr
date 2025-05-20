require "./exchange"

module LavinMQ
  module AMQP
    class HeadersExchange < Exchange
      @bindings = Hash(Hash(String, AMQP::Field), Set(Destination)).new do |h, k|
        h[k] = Set(Destination).new
      end

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
        @bindings.each.flat_map do |args, ds|
          ds.map do |d|
            binding_key = BindingKey.new("", AMQP::Table.new(args))
            BindingDetails.new(name, vhost.name, binding_key, d)
          end
        end
      end

      def bind(destination : Destination, routing_key, headers)
        args = headers || AMQP::Table.new
        validate!(args)
        return false unless @bindings[args.to_h].add? destination
        binding_key = BindingKey.new(routing_key, args)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Bind, data)
        true
      end

      def unbind(destination : Destination, routing_key, headers)
        args = headers || AMQP::Table.new
        bds = @bindings[args.to_h]
        return false unless bds.delete(destination)
        @bindings.delete(routing_key) if bds.empty?

        binding_key = BindingKey.new(routing_key, args)
        data = BindingDetails.new(name, vhost.name, binding_key, destination)
        notify_observers(ExchangeEvent::Unbind, data)

        delete if @auto_delete && @bindings.each_value.all?(&.empty?)
        true
      end

      private def validate!(headers) : Nil
        if h = headers
          if match = h["x-match"]?
            if match != "all" && match != "any"
              raise LavinMQ::Error::PreconditionFailed.new("x-match must be 'any' or 'all'")
            end
          end
        end
      end

      protected def each_destination(routing_key : String, headers : AMQP::Table?, & : LavinMQ::Destination ->)
        default_x_match = @arguments["x-match"]?
        @bindings.each do |args, destinations|
          if headers.nil? || headers.empty?
            next unless args.empty?
            destinations.each do |destination|
              yield destination
            end
          else
            x_match = (args["x-match"]? || default_x_match) == "any" ? :any : :all
            next unless deep_match?(args, headers, x_match)
            destinations.each do |destination|
              yield destination
            end
          end
        end
      end

      # Custom method to do deep comparison of Hash and/or Table
      protected def deep_match?(left, right, x_match : Symbol = :all)
        return left == right unless left.is_a? Hash || left.is_a? AMQP::Table
        return false unless right.is_a? Hash || right.is_a? AMQP::Table
        if x_match == :any
          left.any? { |k, v| !k.starts_with?("x-") && deep_match?(v, right[k]?, x_match) }
        else
          left.all? { |k, v| k.starts_with?("x-") || deep_match?(v, right[k]?, x_match) }
        end
      end
    end
  end
end
