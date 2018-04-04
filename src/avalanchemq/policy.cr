module AvalancheMQ
  class Policy
    alias Value = Int64 | String | Bool | Nil
    alias Definition = Hash(String, Value)

    def initialize(@vhost : VHost, @name : String, @pattern : String, @apply_to : String,
                   @definition : Array(Definition), @priority : Int8)
       @re_pattern = Regex.new(pattern)
    end

    def match?(name)
      !@re_pattern.match(name).nil?
    end

    def to_json(json : JSON::Builder)
      {
        name: @name,
        vhost: @vhost.name,
        pattern: @pattern,
        definition: @definition.to_json,
        priority: @priority
      }.to_json(json)
    end

    def self.from_json(vhost : VHost, data : JSON::Any)
      definition = data["definition"].as(Array(Definition))
      self.new vhost, data["name"], data["pattern"], data["apply_to"],
               definition, data["priority"]
    end
  end

  protected def delete
    @log.info "Deleting"
    @vhost.apply AMQP::Queue::Delete.new 0_u16, 0_u16, @name, false, false, false
  end
end
