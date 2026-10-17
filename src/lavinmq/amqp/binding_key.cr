require "../amqp"

module LavinMQ
  module AMQP
    struct BindingKey
      getter routing_key : String
      getter arguments : AMQP::Table? = nil

      def initialize(@routing_key : String, @arguments : AMQP::Table? = nil)
      end

      def properties_key
        if (args = arguments) && !args.empty?
          @hsh ||= begin
            hsh = args.to_h
            Base64.urlsafe_encode(hsh.keys.sort!.map { |k| "#{k}:#{hsh[k]}" }.join(","))
          end
          return "#{routing_key}~#{@hsh}"
        end
        return "~" if routing_key.empty?
        routing_key
      end

      def_hash properties_key

      # Not the default struct equality: that would also compare the
      # properties_key cache, so a key whose hash had been computed would
      # differ from an identical fresh one
      def ==(other : BindingKey) : Bool
        @routing_key == other.routing_key && @arguments == other.arguments
      end
    end
  end
end
