module AvalancheMQ

  module PolicyTarget
    getter policy

    abstract def apply_policy(@policy : Policy)
  end
  class Policy
    alias Value = Int32 | String | Bool | Nil
    APPLY_TO = ["all", "exchanges", "queues"]
    getter name, apply_to, definition, priority
    def_equals_and_hash @vhost.name, @name

    def initialize(@vhost : VHost, @name : String, @pattern : String, @apply_to : String,
                   @definition : Hash(String, Value), @priority : Int8)
      validate!
      @re_pattern = Regex.new(pattern)
    end

    protected def validate!
      unless APPLY_TO.includes?(@apply_to)
        raise ArgumentError.new("apply_to not any of #{APPLY_TO}")
      end
      pattern_error = Regex.error?(@pattern)
      raise ArgumentError.new("pattern: #{pattern_error}") unless pattern_error.nil?
    end

    def match?(resource : Queue | Exchange)
      match = !@re_pattern.match(resource.name).nil?
      case resource
      when Queue
        match && @apply_to == APPLY_TO[0] || @apply_to == APPLY_TO[2]
      when Exchange
        match && @apply_to == APPLY_TO[0] || @apply_to == APPLY_TO[1]
      end
    end

    def to_json(json : JSON::Builder)
      {
        vhost: @vhost.name,
        name: @name,
        pattern: @pattern,
        definition: @definition.to_json,
        priority: @priority,
        apply_to: @apply_to
      }.to_json(json)
    end

    def self.from_json(vhost : VHost, data : JSON::Any)
      definitions = Hash(String, Value).new
      data["definition"].as_h.each do |k, v|
        val = case v
              when Int64, Float64
                v.to_i32
              when String, Nil
                v.to_s
              when Bool
                v
              else
                raise ArgumentError.new("Invalid definition")
              end
        definitions[k] = val
      end
      self.new vhost, data["name"].as_s, data["pattern"].as_s, data["apply_to"].as_s,
               definitions, data["priority"].as_i.to_i8
    end
  end

  protected def delete
    @log.info "Deleting"
    @vhost.remove_policy(name)
  end
end
