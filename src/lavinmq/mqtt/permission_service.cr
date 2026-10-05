require "json"
require "./permission_group"
require "./topic_rule_segment"

module LavinMQ
  module MQTT
    # Every change rebuilds the compiled state and publishes it with one
    # reference assignment, so readers never see stale or partial state.
    #
    # A topic is allowed only when a rule grants it. A vhost is still open by
    # default: the DEFAULT_GROUP grants every user every topic, and the
    # operator locks the vhost down by deleting or narrowing that group.
    class PermissionService
      Log = LavinMQ::Log.for "mqtt.permission_service"

      DEFAULT_GROUP = PermissionGroup::DEFAULT_NAME

      # Keep disk failures distinct from client IO errors, which the HTTP
      # handler treats as disconnected clients instead of failed requests.
      class SaveError < Exception
      end

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

      # Keep both indexes behind one reference for concurrent readers.
      class Compiled
        getter by_member : Hash(String, Array(CompiledRule))
        getter global_rules : Array(CompiledRule)

        def initialize(@by_member : Hash(String, Array(CompiledRule)),
                       @global_rules : Array(CompiledRule))
        end
      end

      @save_lock = Mutex.new
      @compiled : Compiled

      # A vhost without mqtt_permissions.json gets the DEFAULT_GROUP unless
      # default_group is false, and the result is saved at once, so the file
      # always holds the groups that are in effect.
      def initialize(@vhost : String, @data_dir : String, @replicator : Clustering::Replicator?, default_group = true)
        @groups = Hash(String, PermissionGroup).new
        @compiled = Compiled.new(Hash(String, Array(CompiledRule)).new, Array(CompiledRule).new)
        load!(default_group)
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

      def put(group : PermissionGroup) : PermissionGroup
        group.validate!
        @save_lock.synchronize do
          groups = @groups.dup
          groups[group.name] = group
          commit(groups)
        end
        group
      end

      # Commit the group only when its name is free, so a concurrent create or
      # import cannot be overwritten. Returns false when the name is taken.
      def create(group : PermissionGroup) : Bool
        group.validate!
        @save_lock.synchronize do
          return false if @groups[group.name]?
          groups = @groups.dup
          groups[group.name] = group
          commit(groups)
          true
        end
      end

      # Read the group and commit the change under one lock, so an edit that
      # commits while the caller prepares its own change is not overwritten.
      # The block returns nil to leave the group as it is. Returns false when
      # no group has that name. The block runs with the lock held, so it must
      # not call back into the service and it must not wait on IO.
      def update(name : String, & : PermissionGroup -> PermissionGroup?) : Bool
        @save_lock.synchronize do
          return false unless current = @groups[name]?
          return true unless updated = yield current
          updated.validate!
          groups = @groups.dup
          groups[name] = updated
          commit(groups)
          true
        end
      end

      def delete(name : String) : PermissionGroup?
        @save_lock.synchronize do
          if group = @groups[name]?
            groups = @groups.dup
            groups.delete(name)
            commit(groups)
            group
          end
        end
      end

      # Commit all imported groups together. Check existing names under the
      # same lock as API edits. An import adds and replaces groups, it never
      # deletes one.
      def import(imported : Array(PermissionGroup), skip_existing = false) : Nil
        return if imported.empty?
        imported.each(&.validate!)
        @save_lock.synchronize do
          groups = @groups.dup
          imported.each do |group|
            next if skip_existing && @groups[group.name]?
            groups[group.name] = group
          end
          commit(groups) unless groups == @groups
        end
      end

      def can_write?(context : Context, topic : String) : Bool
        matches?(context, topic, write: true)
      end

      def can_read?(context : Context, topic : String) : Bool
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
          # A group with no valid rule grants nothing, so its members need no entry.
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

      # Groups are validated on load as they are on put, so everything in
      # memory always passes validate! and every later put of a loaded group
      # can only fail on the change being made.
      private def load!(default_group : Bool)
        path = File.join(@data_dir, "mqtt_permissions.json")
        return create_groups(default_group) unless File.exists? path
        File.open(path) do |f|
          Array(PermissionGroup).from_json(f) do |group|
            @groups[group.name] = group.validate!
          end
          @replicator.try &.register_file f
        end
        rebuild
      rescue ex
        Log.error(exception: ex) { "Failed to load permission groups" }
        raise ex
      end

      # Called only when mqtt_permissions.json is missing, that is for a new
      # vhost or one from a version without topic permissions. A deleted
      # default group stays deleted, because the delete leaves an empty list
      # on disk.
      private def create_groups(default_group : Bool) : Nil
        groups = Hash(String, PermissionGroup).new
        groups[DEFAULT_GROUP] = PermissionGroup.default(@vhost) if default_group
        @save_lock.synchronize { commit(groups) }
      end

      # Called with @save_lock held. Build and save a separate collection so
      # permission checks keep using the old state until the rename succeeds.
      #
      # Assign @groups before the replicator call. That call writes to the
      # follower sockets and can suspend this fiber while the lock is still
      # held, and a reader that runs then must see the committed groups.
      private def commit(groups : Hash(String, PermissionGroup)) : Nil
        path = save!(groups)
        @groups = groups
        rebuild
        @replicator.try &.replace_file path
      end

      private def save!(groups : Hash(String, PermissionGroup)) : String
        path = File.join(@data_dir, "mqtt_permissions.json")
        tmpfile = "#{path}.tmp"
        File.open(tmpfile, "w") do |f|
          groups.values.to_pretty_json(f)
          f.fsync
        end
        File.rename tmpfile, path
        path
      rescue ex : IO::Error
        raise SaveError.new("Failed to save MQTT permission groups for vhost #{@vhost.inspect}", cause: ex)
      end
    end
  end
end
