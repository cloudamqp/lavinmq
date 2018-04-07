module AvalancheMQ
  module ParameterTarget
    getter paramter

    abstract def apply_parameters(@paramter : Parameter)
  end

  class Parameter
    alias Value = Int64 | String | Bool | Nil

    def initialize(@component_name : String, @name : String, @value : JSON::Any)
    end

    def apply?(component_name, name)
      component_name == @component_name && name == @name
    end
  end
end
