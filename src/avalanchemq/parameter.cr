module AvalancheMQ

  module ParameterTarget
    abstract def apply_parameter(p : Parameter)
  end

  alias ParameterId = { String, String } | String
  alias ParameterValue = JSON::Type | Hash(String, ParameterValue) | Array(ParameterValue)
  class Parameter
    def_equals_and_hash @name
    getter name, component_name, parameter_name, value

    def initialize(@component_name : String, @parameter_name : String, @value : JSON::Any)
      @name = { @component_name, @parameter_name }
    end

    def to_json(json : JSON::Builder)
      {
        "component-name": @component_name,
        "parameter-name": @parameter_name,
        "value": @value
      }.to_json(json)
    end

    def self.from_json(data : JSON::Any)
      self.new(data["component-name"].as_s, data["parameter-name"].as_s, data["value"])
    end
  end
end
