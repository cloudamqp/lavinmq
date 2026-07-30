require "json"
require "../mqtt/topic_filter"

module LavinMQ
  module Auth
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
      getter protocol : String = "mqtt"
      getter members : Array(String) = [] of String
      getter rules : Array(Rule) = [] of Rule

      def initialize(@name : String,
                     @protocol : String = "mqtt",
                     @members : Array(String) = [] of String,
                     @rules : Array(Rule) = [] of Rule)
      end

      def validate! : self
        unless @protocol == "mqtt"
          raise ArgumentError.new("Unsupported protocol #{@protocol.inspect} in permission group #{@name.inspect}, only 'mqtt' is supported")
        end
        @rules.each do |rule|
          unless MQTT::TopicFilter.valid_filter?(rule.pattern)
            raise ArgumentError.new("Invalid MQTT topic filter #{rule.pattern.inspect} in permission group #{@name.inspect}")
          end
        end
        self
      end
    end
  end
end
