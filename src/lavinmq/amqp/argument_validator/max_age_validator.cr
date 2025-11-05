module LavinMQ
  module AMQP
    module ArgumentValidator
      struct MaxAgeValidator
        include ArgumentValidator
        VALID_MAX_AGE_PATTERN = %r{\A\d+[YMDhms]\z}

        def validate!(header : String, value : AMQP::Field) : Nil
          max_age = value.as?(String) || raise_invalid! "#{header} must be a string"
          max_age.matches?(VALID_MAX_AGE_PATTERN) || raise_invalid! "#{header} format invalid"
          nil
        end
      end
    end
  end
end
