require "json"
require "./sortable_json"

module AvalancheMQ
  module PolicyTarget
    abstract def apply_policy(p : Policy)
    abstract def clear_policy
    abstract def policy : Policy?
  end

  class Policy
    enum Target
      All
      Queues
      Exchanges

      def to_json(json : JSON::Builder)
        to_s.downcase.to_json(json)
      end
    end

    include SortableJSON
    include JSON::Serializable

    getter name : String
    getter vhost : String
    getter pattern : Regex
    @[JSON::Field(key: "apply-to")]
    getter apply_to : Target
    getter priority : Int8
    getter definition : Hash(String, JSON::Any)

    def initialize(@name : String, @vhost : String, @pattern : Regex, @apply_to : Target,
                   @definition : Hash(String, JSON::Any), @priority : Int8)
    end

    def match?(resource : Queue | Exchange)
      applies = case resource
                when Queue
                  @apply_to == Target::Queues || @apply_to == Target::All
                when Exchange
                  @apply_to == Target::Exchanges || @apply_to == Target::All
                end
      applies && !@pattern.match(resource.name).nil?
    end

    def details_tuple
      {
        name: @name,
        vhost: @vhost,
        pattern: @pattern,
        "apply-to": @apply_to,
        priority: @priority,
        definition: @definition
      }
    end
  end
end
