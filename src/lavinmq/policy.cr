require "json"
require "./sortable_json"

module LavinMQ
  module PolicyTarget
    abstract def apply_policy(policy : Policy)
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

    def match?(resource : Queue)
      return false if @apply_to.exchanges?
      @pattern.matches?(resource.name)
    end

    def match?(resource : Exchange)
      return false if @apply_to.queues?
      @pattern.matches?(resource.name)
    end

    def details_tuple
      {
        name:       @name,
        vhost:      @vhost,
        pattern:    @pattern,
        "apply-to": @apply_to,
        priority:   @priority,
        definition: @definition,
      }
    end
  end
end
