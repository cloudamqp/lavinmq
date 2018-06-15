require "json"

module AvalancheMQ
  module ParameterTarget
    abstract def add_parameter(p : Parameter)
    abstract def apply_parameter(p : Parameter?)
    abstract def delete_parameter(component : String?, name : String)
  end

  alias ParameterId = {String?, String} | String

  class Parameter
    def_equals_and_hash @name
    getter component_name, parameter_name, value

    JSON.mapping(
      component_name: {key: "component", setter: false, type: String?},
      parameter_name: {key: "name", setter: false, type: String},
      value: {setter: false, type: JSON::Any}
    )

    @name : ParameterId?

    def initialize(@component_name : String?, @parameter_name : String, @value : JSON::Any)
      @name = {@component_name, @parameter_name}
    end

    def name
      @name ||= {@component_name, @parameter_name}
      @name
    end
  end
end
