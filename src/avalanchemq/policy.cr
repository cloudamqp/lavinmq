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

    SortableJSON.mapping(
      name: {type: String, setter: false},
      vhost: {type: String, setter: false},
      pattern: {type: Regex, setter: false},
      apply_to: {key: "apply-to", type: Target, setter: false},
      definition: {type: Hash(String, JSON::Any), setter: false},
      priority: {type: Int8, setter: false}
    )

    getter name, vhost, definition, pattern, apply_to, priority

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
  end
end
