require "json"
require "../../../src/lavinmq/policy_pattern"

module LavinMQWasm
  struct Policy
    include JSON::Serializable

    getter name : String
    getter pattern : String
    @[JSON::Field(key: "apply_to")]
    getter apply_to : String # "queues", "exchanges", "all"
    getter priority : Int32
    getter definition : Hash(String, JSON::Any)

    def matches?(resource_name : String, resource_type : String) : Bool
      case apply_to
      when "queues"    then return false if resource_type == "exchange"
      when "exchanges" then return false if resource_type == "queue"
      end
      LavinMQ::PolicyPattern.matches?(pattern, resource_name)
    end
  end

  def self.match_policies(
    policies : Array(Policy),
    resource_name : String,
    resource_type : String,
  ) : {Policy?, Hash(String, JSON::Any)}
    matched = policies
      .select(&.matches?(resource_name, resource_type))
      .max_by?(&.priority)

    effective_args = matched ? matched.definition.dup : Hash(String, JSON::Any).new
    {matched, effective_args}
  end
end
