require "json"
require "./permission_group"
require "../mqtt/topic_rule_segment"

module LavinMQ
  module Auth
    # Every change rebuilds the compiled state and publishes it with one
    # reference assignment, so readers never see stale or partial state.
    class PermissionService
      Log = LavinMQ::Log.for "permission_service"

      record CompiledRule,
        chain : MQTT::TopicRuleSegment,
        read : Bool,
        write : Bool

      class Compiled
        getter by_member : Hash(String, Array(CompiledRule))
        getter wildcard_rules : Array(CompiledRule)
        getter? mqtt_in_use : Bool

        def initialize(@by_member : Hash(String, Array(CompiledRule)),
                       @wildcard_rules : Array(CompiledRule),
                       @mqtt_in_use : Bool)
        end
      end

      @save_lock = Mutex.new
      @compiled : Compiled

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @groups = Hash(String, PermissionGroup).new
        @compiled = Compiled.new(Hash(String, Array(CompiledRule)).new, [] of CompiledRule, false)
        load!
      end

      def []?(name : String) : PermissionGroup?
        @groups[name]?
      end

      def values : Array(PermissionGroup)
        @groups.values
      end

      def mqtt_in_use? : Bool
        @compiled.mqtt_in_use?
      end

      def put(group : PermissionGroup, save = true) : PermissionGroup
        group.validate!
        @groups[group.name] = group
        rebuild
        save! if save
        group
      end

      def delete(name : String, save = true) : PermissionGroup?
        if group = @groups.delete(name)
          rebuild
          save! if save
          group
        end
      end

      def can_write?(client_id : String, topic : String) : Bool
        return true unless mqtt_in_use?
        matches?(client_id, topic, write: true)
      end

      def can_read?(client_id : String, topic : String) : Bool
        return true unless mqtt_in_use?
        matches?(client_id, topic, write: false)
      end

      def to_json(json : JSON::Builder)
        @groups.values.to_json(json)
      end

      # Read @compiled once so a concurrent rebuild is never observed halfway.
      private def matches?(client_id : String, topic : String, write : Bool) : Bool
        compiled = @compiled
        return true if rules_match?(compiled.wildcard_rules, client_id, topic, write)
        return false unless own = compiled.by_member[client_id]?
        rules_match?(own, client_id, topic, write)
      end

      private def rules_match?(rules : Array(CompiledRule), client_id : String, topic : String, write : Bool) : Bool
        rules.each do |rule|
          next unless write ? rule.write : rule.read
          return true if MQTT::TopicRuleSegment.matches?(rule.chain, topic, client_id)
        end
        false
      end

      private def rebuild : Nil
        by_member = Hash(String, Array(CompiledRule)).new
        wildcard_rules = [] of CompiledRule
        mqtt_in_use = false
        @groups.each_value do |group|
          next unless group.protocol == "mqtt"
          compiled_rules = [] of CompiledRule
          group.rules.each do |rule|
            chain = MQTT::TopicRuleSegment.compile(rule.pattern)
            if chain.nil?
              Log.warn { "Ignoring invalid topic filter #{rule.pattern.inspect} in permission group #{group.name.inspect}" }
              next
            end
            compiled_rules << CompiledRule.new(chain, rule.read?, rule.write?)
          end
          # A group with no valid rule grants nothing and must not be what
          # flips every client to default-deny.
          next if compiled_rules.empty?
          mqtt_in_use = true
          if group.members.includes?("*")
            wildcard_rules.concat(compiled_rules)
          else
            group.members.each do |member|
              (by_member[member] ||= [] of CompiledRule).concat(compiled_rules)
            end
          end
        end
        @compiled = Compiled.new(by_member, wildcard_rules, mqtt_in_use)
      end

      private def load!
        path = File.join(@data_dir, "permission_groups.json")
        return unless File.exists? path
        File.open(path) do |f|
          Array(PermissionGroup).from_json(f) do |group|
            @groups[group.name] = group
          end
          @replicator.try &.register_file f
        end
        rebuild
      rescue ex
        Log.error(exception: ex) { "Failed to load permission groups" }
        raise ex
      end

      def save!
        path = File.join(@data_dir, "permission_groups.json")
        tmpfile = "#{path}.tmp"
        @save_lock.synchronize do
          File.open(tmpfile, "w") do |f|
            to_pretty_json(f)
            f.fsync
          end
          File.rename tmpfile, path
        end
        @replicator.try &.replace_file path
      end
    end
  end
end
