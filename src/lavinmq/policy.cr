require "json"
require "./sortable_json"

module LavinMQ
  module PolicyTarget
    abstract def apply_policy(policy : Policy?, operator_policy : OperatorPolicy?)
    abstract def clear_policy
    abstract def policy : Policy?
    abstract def operator_policy : OperatorPolicy?
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

    def self.merge_definitions(p1 : Policy?, p2 : Policy?) : Hash(String, JSON::Any)
      if d1 = p1.try &.definition
        if d2 = p2.try &.definition
          merged = Hash(String, JSON::Any).new
          Iterator.chain({d1.each, d2.each}).each do |k, v|
            if value = v.as_i64?
              merged[k] = v unless merged[k]?.try(&.as_i64?.try { |i| i < value })
            else
              merged[k] = v
            end
          end
          merged
        else
          d1
        end
      elsif d2 = p2.try &.definition
        d2
      else
        Hash(String, JSON::Any).new
      end
    end
  end

  class OperatorPolicy < Policy; end
end
