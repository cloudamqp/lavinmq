require "json"
require "./topic_filter"

module LavinMQ
  module MQTT
    class PermissionGroup
      include JSON::Serializable

      # Rule identifiers make individual rules addressable in the HTTP API.
      IDENTIFIER_PATTERN = /\A[A-Za-z0-9-]+\z/

      struct Rule
        include JSON::Serializable
        getter identifier : String
        getter pattern : String
        getter? read : Bool = false
        getter? write : Bool = false

        def initialize(@identifier : String, @pattern : String, @read : Bool = false, @write : Bool = false)
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
        identifiers = Set(String).new
        @rules.each do |rule|
          unless rule.identifier.matches?(IDENTIFIER_PATTERN)
            raise ArgumentError.new("Invalid rule identifier #{rule.identifier.inspect} in permission group #{@name.inspect}, only alphanumerics and hyphens are allowed")
          end
          unless identifiers.add?(rule.identifier)
            raise ArgumentError.new("Duplicate rule identifier #{rule.identifier.inspect} in permission group #{@name.inspect}")
          end
          unless TopicFilter.valid_filter?(rule.pattern)
            raise ArgumentError.new("Invalid MQTT topic filter #{rule.pattern.inspect} in permission group #{@name.inspect}")
          end
        end
        self
      end
    end
  end
end
