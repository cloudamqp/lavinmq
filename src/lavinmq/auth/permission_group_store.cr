require "json"
require "./permission_group"

module LavinMQ
  module Auth
    class PermissionGroupStore
      Log = LavinMQ::Log.for "permission_group_store"

      @save_lock = Mutex.new
      @revision = Atomic(UInt32).new(0_u32)

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @groups = Hash(String, PermissionGroup).new
        load!
      end

      def []?(name : String) : PermissionGroup?
        @groups[name]?
      end

      def [](name : String) : PermissionGroup
        @groups[name]
      end

      def values : Array(PermissionGroup)
        @groups.values
      end

      def revision : UInt32
        @revision.get
      end

      def put(group : PermissionGroup, save = true) : PermissionGroup
        @groups[group.name] = group
        @revision.add(1, :relaxed)
        save! if save
        group
      end

      def delete(name : String, save = true) : PermissionGroup?
        if group = @groups.delete(name)
          @revision.add(1, :relaxed)
          save! if save
          group
        end
      end

      # Groups an MQTT-connected user resolves: mqtt protocol and member (or apply_to_all).
      def for_mqtt_user(username : String) : Array(PermissionGroup)
        @groups.values.select { |g| g.protocol == "mqtt" && g.member?(username) }
      end

      def to_json(json : JSON::Builder)
        @groups.values.to_json(json)
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
      rescue ex
        Log.error(exception: ex) { "Failed to load permission groups" }
        raise ex
      end

      def save!
        path = File.join(@data_dir, "permission_groups.json")
        tmpfile = "#{path}.tmp"
        @save_lock.synchronize do
          File.open(tmpfile, "w") do |f|
            JSON.build(f, indent: "  ") { |json| to_json(json) }
            f.fsync
          end
          File.rename tmpfile, path
        end
        @replicator.try &.replace_file path
      end
    end
  end
end
