require "json"
require "./topic_filter"

module LavinMQ
  module MQTT
    class PermissionGroup
      include JSON::Serializable

      struct Rule
        include JSON::Serializable
        getter pattern : String
        getter? read : Bool = false
        getter? write : Bool = false

        def initialize(@pattern : String, @read : Bool = false, @write : Bool = false)
        end
      end

      getter name : String
      getter vhost : String
      getter members = Array(String).new
      getter rules = Array(Rule).new

      def initialize(@name : String,
                     @vhost : String,
                     @members = Array(String).new,
                     @rules = Array(Rule).new)
      end

      def validate! : self
        @rules.each do |rule|
          unless TopicFilter.valid_filter?(rule.pattern)
            raise ArgumentError.new("Invalid MQTT topic filter #{rule.pattern.inspect} in permission group #{@name.inspect}")
          end
        end
        self
      end
    end
  end
end
