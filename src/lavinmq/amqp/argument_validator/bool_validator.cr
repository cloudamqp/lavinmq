module LavinMQ
  module AMQP
    module ArgumentValidator
      struct BoolValidator
        include ArgumentValidator

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          value.is_a?(Bool) || raise_invalid!("#{header} header not a boolean")
          nil
        end
      end
    end
  end
end
