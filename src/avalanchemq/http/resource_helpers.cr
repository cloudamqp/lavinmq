module AvalancheMQ
  module HTTP
    module ResourceHelpers
      private def parse_arguments(body) : Hash(String, AMQP::Field)
        if args = body["arguments"]?.try(&.as_h?)
          cast_to_field(args).as Hash(String, AMQP::Field)
        else
          Hash(String, AMQP::Field).new
        end
      end

      # https://github.com/crystal-lang/crystal/issues/4885#issuecomment-325109328
      def cast_to_field(x : Array) : AMQP::Field
        x.map { |e| cast_to_field(e).as(AMQP::Field) }.as(AMQP::Field)
      end

      def cast_to_field(x : Hash) : AMQP::Field
        h = Hash(String, AMQP::Field).new
        x.each do |(k, v)|
          h[k] = cast_to_field(v).as(AMQP::Field)
        end
        h.as(AMQP::Field)
      end

      def cast_to_field(x : JSON::Any) : AMQP::Field
        if a = x.as_a?
          cast_to_field(a)
        elsif h = x.as_h?
          cast_to_field(h)
        else
          x.raw.as(AMQP::Field)
        end
      end
    end
  end
end
