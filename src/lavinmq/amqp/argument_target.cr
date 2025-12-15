require "./argument_validator"

module LavinMQ
  module AMQP
    module ArgumentTarget
      # By having this as a macro we can make some compile-time validations to
      # prevent bugs like adding the same key twice
      macro add_argument_validator(key, validator)
        {%
          raise "#{key} already added" if ARGUMENT_VALIDATORS.has_key?(key)
          ARGUMENT_VALIDATORS[key] = validator
        %}
      end

      macro included
        # __nil is a workaround for not being able to declare an empty namedtuple
        # and populate it from macro
        ARGUMENT_VALIDATORS = {__nil: nil}

        def self.validate_arguments!(arguments : AMQP::Table)
          arguments.each do |k, v|
            if validator = ARGUMENT_VALIDATORS[k]?
              validator.validate!(k, v, arguments)
            end
          end
        end
      end
    end
  end
end
