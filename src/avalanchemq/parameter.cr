module AvalancheMQ

  module ParameterTarget
    abstract def apply_parameter(p : Parameter)
  end

  alias ParameterId = { String, String } | String
  alias ParameterValue = JSON::Type | Hash(String, ParameterValue) | Array(ParameterValue)
  class Parameter
    def_equals_and_hash @name
    getter component_name, parameter_name, value

    JSON.mapping(
      component_name: { key: "component-name", setter: false, type: String },
      parameter_name: { key: "parameter-name", setter: false, type: String },
      value: { setter: false, type: JSON::Any }
    )

    @name : ParameterId?

    def initialize(@component_name : String, @parameter_name : String, @value : JSON::Any)
      @name = { @component_name, @parameter_name }
    end

    def name
      @name ||= { @component_name, @parameter_name }
      @name
    end
  end
end
