require "./topic_filter_set"
require "../auth/permission_group"

module LavinMQ
  module MQTT
    class TopicPermissions
      getter read : TopicFilterSet
      getter write : TopicFilterSet

      def initialize(@read : TopicFilterSet, @write : TopicFilterSet)
      end

      def self.build(groups : Array(Auth::PermissionGroup),
                     username : String) : TopicPermissions
        read = TopicFilterSet.new
        write = TopicFilterSet.new
        groups.each do |group|
          group.rules.each do |rule|
            if rule.pattern.includes?("{username}")
              # Fail closed: skip the rule rather than expand it into an over-broad filter.
              next unless TopicFilterSet.valid_substitution?(username)
            end
            pattern = TopicFilterSet.expand(rule.pattern, username)
            read.add(pattern) if rule.read?
            write.add(pattern) if rule.write?
          end
        end
        new(read, write)
      end
    end
  end
end
