module LavinMQ
  module AMQP
    module ArgumentValidator
      struct StringValidator
        include ArgumentValidator

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          value.as?(String) || raise_invalid!("#{header} header not a string")
          nil
        end
      end
    end
  end
end
