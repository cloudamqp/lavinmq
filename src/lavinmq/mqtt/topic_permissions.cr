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
                     username : String,
                     client_id : String) : TopicPermissions
        read = TopicFilterSet.new
        write = TopicFilterSet.new
        groups.each do |group|
          group.rules.each do |rule|
            pattern = TopicFilterSet.expand(rule.pattern, username, client_id)
            read.add(pattern) if rule.read
            write.add(pattern) if rule.write
          end
        end
        new(read, write)
      end
    end
  end
end
