module LavinMQ
  module AMQP
    module ArgumentValidator
      # Validates that value is an Int and optionally greater and/or smaller than given values
      struct IntValidator
        include ArgumentValidator

        def initialize(@min_value : Int32? = nil, @max_value : Int32? = nil)
        end

        def validate!(header : String, value : AMQP::Field) : Nil
          return if value.nil?
          int_value = value.as?(Int) || raise_invalid!("#{header} header not an integer")
          if (min_value = @min_value) && int_value < min_value
            raise_invalid!("#{header} header less than minimum value #{min_value}")
          end
          if (max_value = @max_value) && int_value > max_value
            raise_invalid!("#{header} header greater than maximum value #{max_value}")
          end
          nil
        end
      end
    end
  end
end
