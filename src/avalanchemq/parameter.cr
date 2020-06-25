require "json"

module AvalancheMQ
  module ParameterTarget
    abstract def add_parameter(p : Parameter)
    abstract def apply_parameter(p : Parameter?)
    abstract def delete_parameter(component : String?, name : String)
  end

  alias ParameterId = {String?, String} | String

  class Parameter
    include JSON::Serializable

    @[JSON::Field(key: "component")]
    getter component_name : String?
    @[JSON::Field(key: "name")]
    getter parameter_name : String
    @[JSON::Field(key: "value")]
    property value

    def initialize(@component_name : String?, @parameter_name : String, @value : JSON::Any)
    end

    def name
      { @component_name, @parameter_name }
    end
  end
end
