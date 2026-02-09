require "json"
require "sync/shared"
require "./user"

module LavinMQ
  module Auth
    class UserStore
      include Enumerable({String, User})
      private DIRECT_USER = "__direct"
      Log         = LavinMQ::Log.for "user_store"

      def direct_user
        @users.shared { |h| h[DIRECT_USER] }
      end

      def self.hidden?(name)
        DIRECT_USER == name
      end

      @users : Sync::Shared(Hash(String, User))

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @users = Sync::Shared.new(Hash(String, User).new)
        load!
      end

      # Explicit accessors replacing forward_missing_to

      def []?(name : String) : User?
        @users.shared { |h| h[name]? }
      end

      def [](name : String) : User
        @users.shared { |h| h[name] }
      end

      def each_value(& : User ->)
        @users.shared { |h| h.each_value { |v| yield v } }
      end

      def each_value : Iterator(User)
        @users.shared(&.values).each
      end

      def has_key?(name : String) : Bool
        @users.shared(&.has_key?(name))
      end

      def size : Int32
        @users.shared(&.size)
      end

      def values : Array(User)
        @users.shared(&.values)
      end

      def compact_map(& : {String, User} -> U?) : Array(U) forall U
        @users.shared { |h| h.compact_map { |kv| yield kv } }
      end

      def select(*args)
        @users.shared(&.select(*args))
      end

      def each(&)
        @users.shared do |h|
          h.each do |kv|
            yield kv
          end
        end
      end

      # Adds a user to the user store
      def create(name, password, tags = Array(Tag).new, save = true)
        @users.lock do |h|
          if user = h[name]?
            return user
          end
          user = User.create(name, password, "SHA256", tags)
          h[name] = user
          Log.info { "Created user=#{name}" }
          save! if save
          user
        end
      end

      def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true)
        user = User.new(name, password_hash, password_algorithm, tags)
        @users.lock { |h| h[name] = user }
        save! if save
        user
      end

      def add_permission(user : User, vhost, config, read, write)
        add_permission(user.name, vhost, config, read, write)
      end

      def add_permission(user, vhost, config, read, write)
        perm = {config: config, read: read, write: write}
        @users.shared do |h|
          if h[user].permissions[vhost]? && h[user].permissions[vhost] == perm
            return perm
          end
          h[user].permissions[vhost] = perm
          h[user].clear_permissions_cache
        end
        save!
        perm
      end

      def rm_permission(user, vhost)
        perm = @users.shared do |h|
          if p = h[user].permissions.delete(vhost)
            h[user].clear_permissions_cache
            p
          end
        end
        if perm
          Log.info { "Removed permissions for user=#{user} on vhost=#{vhost}" }
          save!
          perm
        end
      end

      def rm_vhost_permissions_for_all(vhost)
        @users.shared do |h|
          h.each_value do |user|
            user.permissions.delete(vhost)
            user.clear_permissions_cache
          end
        end
        save!
      end

      def delete(name, save = true) : User?
        return if name == DIRECT_USER
        user = @users.lock(&.delete(name))
        if user
          user.permissions.clear
          user.clear_permissions_cache
          Log.info { "Deleted user=#{name}" }
          save! if save
          user
        end
      end

      def default_user : User
        @users.shared do |h|
          h.each_value do |u|
            if u.tags.includes?(Tag::Administrator) && !u.hidden?
              return u
            end
          end
          h.each_value do |u|
            if u.tags.includes?(Tag::Administrator)
              return u
            end
          end
        end
        raise "No user with administrator privileges found"
      end

      def to_json(json : JSON::Builder)
        json.array do
          each_value do |user|
            next if user.hidden?
            user.to_json(json)
          end
        end
      end

      private def load!
        path = File.join(@data_dir, "users.json")
        if File.exists? path
          Log.debug { "Loading users from file" }
          File.open(path) do |f|
            @users.lock do |h|
              Array(User).from_json(f) do |user|
                h[user.name] = user
              end
            end
            @replicator.try &.register_file f
          end
        else
          Log.debug { "Loading default users" }
          create_default_user
        end
        create_direct_user
        Log.debug { "#{size} users loaded" }
      rescue ex
        Log.error(exception: ex) { "Failed to load users" }
        raise ex
      end

      private def create_default_user
        add(Config.instance.default_user, Config.instance.default_password_hash.to_s, "SHA256", tags: [Tag::Administrator], save: false)
        add_permission(Config.instance.default_user, "/", /.*/, /.*/, /.*/)
        save!
      end

      private def create_direct_user
        @users.lock do |h|
          h[DIRECT_USER] = User.create_hidden_user(DIRECT_USER)
          perm = {config: /.*/, read: /.*/, write: /.*/}
          h[DIRECT_USER].permissions["/"] = perm
        end
      end

      def save!
        Log.debug { "Saving users to file" }
        path = File.join(@data_dir, "users.json")
        tmpfile = "#{path}.tmp"
        File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
        File.rename tmpfile, path
        @replicator.try &.replace_file path
      end
    end
  end
end
