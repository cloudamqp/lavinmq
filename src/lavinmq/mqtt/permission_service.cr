require "json"
require "./permission_group"
require "./topic_rule_segment"

module LavinMQ
  module MQTT
    # Every change rebuilds the compiled state and publishes it with one
    # reference assignment, so readers never see stale or partial state.
    class PermissionService
      Log = LavinMQ::Log.for "mqtt.permission_service"

      record CompiledRule,
        chain : TopicRuleSegment,
        read : Bool,
        write : Bool

      # What a check knows about the connection. The username selects the
      # member rules, the client id feeds the {client_id} substitution. The
      # username is nil for a restored session no client has attached to yet.
      record Context,
        username : String?,
        client_id : String

      class Compiled
        getter by_member : Hash(String, Array(CompiledRule))
        getter global_rules : Array(CompiledRule)
        getter? empty : Bool

        def initialize(@by_member : Hash(String, Array(CompiledRule)),
                       @global_rules : Array(CompiledRule))
          @empty = @by_member.empty? && @global_rules.empty?
        end
      end

      @save_lock = Mutex.new
      @compiled : Compiled

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @groups = Hash(String, PermissionGroup).new
        @compiled = Compiled.new(Hash(String, Array(CompiledRule)).new, Array(CompiledRule).new)
        load!
      end

      def []?(name : String) : PermissionGroup?
        @groups[name]?
      end

      def values : Array(PermissionGroup)
        @groups.values
      end

      def size : Int32
        @groups.size
      end

      def each_value(& : PermissionGroup ->) : Nil
        @groups.each_value { |group| yield group }
      end

      def in_use? : Bool
        !@compiled.empty?
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

      def can_write?(context : Context, topic : String) : Bool
        return true unless in_use?
        matches?(context, topic, write: true)
      end

      def can_read?(context : Context, topic : String) : Bool
        return true unless in_use?
        matches?(context, topic, write: false)
      end

      def to_json(json : JSON::Builder)
        @groups.values.to_json(json)
      end

      # Read @compiled once so a concurrent rebuild is never observed halfway.
      private def matches?(context : Context, topic : String, write : Bool) : Bool
        compiled = @compiled
        client_id = context.client_id
        return true if rules_match?(compiled.global_rules, client_id, topic, write)
        return false unless username = context.username
        return false unless own = compiled.by_member[username]?
        rules_match?(own, client_id, topic, write)
      end

      private def rules_match?(rules : Array(CompiledRule), client_id : String, topic : String, write : Bool) : Bool
        rules.each do |rule|
          next unless write ? rule.write : rule.read
          return true if TopicRuleSegment.matches?(rule.chain, topic, client_id)
        end
        false
      end

      # A group's compiled rules are stored once and referenced by each of its
      # members. A client in more than one group gets a merged copy.
      private def rebuild : Nil
        by_member = Hash(String, Array(CompiledRule)).new
        global_rules = Array(CompiledRule).new
        @groups.each_value do |group|
          compiled_rules = Array(CompiledRule).new(group.rules.size)
          group.rules.each do |rule|
            chain = TopicRuleSegment.compile(rule.pattern)
            if chain.nil?
              Log.warn { "Ignoring invalid topic filter #{rule.pattern.inspect} in permission group #{group.name.inspect}" }
              next
            end
            compiled_rules << CompiledRule.new(chain, rule.read?, rule.write?)
          end
          # A group with no valid rule grants nothing; skip it so its members
          # don't get empty entries that would flip every client to default-deny.
          next if compiled_rules.empty?
          if group.members.includes?("*")
            global_rules.concat(compiled_rules)
          else
            group.members.each do |member|
              if own = by_member[member]?
                by_member[member] = own + compiled_rules
              else
                by_member[member] = compiled_rules
              end
            end
          end
        end
        @compiled = Compiled.new(by_member, global_rules)
      end

      private def load!
        path = File.join(@data_dir, "mqtt_permissions.json")
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
        path = File.join(@data_dir, "mqtt_permissions.json")
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
