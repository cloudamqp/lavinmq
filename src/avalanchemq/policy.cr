require "./error"
module AvalancheMQ

  module PolicyTarget
    abstract def apply_policy(p : Policy)
    abstract def policy : Policy?
  end
  class Policy
    def_equals_and_hash @name, @vhost
    getter name, vhost, definition, pattern, apply_to, priority
    APPLY_TO = ["all", "exchanges", "queues"]

    def initialize(@name : String, @vhost : String, @pattern : String, @apply_to : String,
                   @definition : Hash(String, ParameterValue), @priority : Int8)
    end

    def validate!
      unless APPLY_TO.includes?(@apply_to)
        raise InvalidDefinitionError.new("apply-to not any of #{APPLY_TO}")
      end
      (e = Regex.error?(@pattern)) && raise InvalidDefinitionError.new("Invalid pattern: #{e}")
    end

    def match?(resource : Queue | Exchange)
      @re_pattern ||= Regex.new(@pattern)
      applies = case resource
                when Queue
                  @apply_to == APPLY_TO[0] || @apply_to == APPLY_TO[2]
                when Exchange
                  @apply_to == APPLY_TO[0] || @apply_to == APPLY_TO[1]
                end
      applies && !@re_pattern.not_nil!.match(resource.name).nil?
    end

    def to_json(json : JSON::Builder)
      {
        vhost: @vhost,
        name: @name,
        pattern: @pattern,
        definition: @definition,
        priority: @priority,
        "apply-to": @apply_to
      }.to_json(json)
    end

    def self.from_json(data : JSON::Any)
      definitions = Hash(String, ParameterValue).new
      data["definition"].as_h.each do |k, v|
        val = case v
              when Int64, Float64
                v.to_i64
              when String, Nil
                v.to_s
              when Bool
                v
              else
                raise InvalidDefinitionError.new("Invalid definition, unknow data type #{v.class}")
              end
        definitions[k] = val
      end
      self.new data["name"].as_s, data["vhost"].as_s, data["pattern"].as_s,
               data["apply-to"].as_s, definitions, data["priority"].as_i.to_i8
    rescue e : KeyError
      raise InvalidJSONError.new("Policy json invalid: #{e.message}")
    end

    class InvalidDefinitionError < ArgumentError; end
  end
end
