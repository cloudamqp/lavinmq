require "json"
require "./topic_filter"

module LavinMQ
  module MQTT
    class PermissionGroup
      include JSON::Serializable

      # Rule identifiers make individual rules addressable in the HTTP API.
      IDENTIFIER_PATTERN = /\A[A-Za-z0-9-]+\z/
      # Group names travel in URL paths; the charset keeps them unambiguous there.
      NAME_PATTERN = /\A[A-Za-z0-9_-]{1,255}\z/
      DEFAULT_NAME = "default"

      struct Rule
        include JSON::Serializable
        getter identifier : String
        getter pattern : String
        getter? read : Bool = false
        getter? write : Bool = false

        def initialize(@identifier : String, @pattern : String, @read : Bool = false, @write : Bool = false)
        end
      end

      # On disk the rules are an array of objects; in memory they are keyed on
      # the identifier, so identifiers are unique by construction.
      module RulesConverter
        def self.from_json(pull : JSON::PullParser) : Hash(String, Rule)
          PermissionGroup.index_rules(Array(Rule).new(pull))
        end

        def self.to_json(rules : Hash(String, Rule), json : JSON::Builder)
          json.array do
            rules.each_value(&.to_json(json))
          end
        end
      end

      getter name : String
      getter vhost : String
      getter members = Array(String).new
      @[JSON::Field(converter: LavinMQ::MQTT::PermissionGroup::RulesConverter)]
      getter rules = Hash(String, Rule).new

      def initialize(@name : String,
                     @vhost : String,
                     @members = Array(String).new,
                     rules = Array(Rule).new)
        @rules = PermissionGroup.index_rules(rules)
      end

      def self.index_rules(rules : Array(Rule)) : Hash(String, Rule)
        indexed = Hash(String, Rule).new(initial_capacity: rules.size)
        rules.each do |rule|
          unless indexed.has_key?(rule.identifier)
            indexed[rule.identifier] = rule
            next
          end
          raise ArgumentError.new("Duplicate rule identifier #{rule.identifier.inspect}")
        end
        indexed
      end

      # The group every vhost gets until somebody configures it: every user may
      # read and write every topic.
      def self.default(vhost : String) : self
        rule = Rule.new("allow-all", "#", read: true, write: true)
        new(DEFAULT_NAME, vhost, ["*"], [rule])
      end

      # Rule is a struct, so fresh containers make the clone independent.
      def clone : self
        PermissionGroup.new(@name, @vhost, @members.dup, @rules.values)
      end

      def add_member(username : String) : Bool
        return false if @members.includes?(username)
        @members << username
        true
      end

      def remove_member(username : String) : Bool
        !@members.delete(username).nil?
      end

      def put_rule(rule : Rule) : Rule?
        validate_rule!(rule)
        replaced = @rules[rule.identifier]?
        @rules[rule.identifier] = rule
        replaced
      end

      def delete_rule(identifier : String) : Rule?
        @rules.delete(identifier)
      end

      def validate! : self
        unless @name.matches?(NAME_PATTERN)
          raise ArgumentError.new("Invalid group name #{@name.inspect}, only alphanumerics, hyphens and underscores are allowed, max 255 characters")
        end
        @rules.each_value { |rule| validate_rule!(rule) }
        self
      end

      private def validate_rule!(rule : Rule) : Nil
        unless rule.identifier.matches?(IDENTIFIER_PATTERN)
          raise ArgumentError.new("Invalid rule identifier #{rule.identifier.inspect} in permission group #{@name.inspect}, only alphanumerics and hyphens are allowed")
        end
        unless TopicFilter.valid_filter?(rule.pattern)
          raise ArgumentError.new("Invalid MQTT topic filter #{rule.pattern.inspect} in permission group #{@name.inspect}")
        end
      end
    end
  end
end
