require "json"
require "./user"

module LavinMQ
  module Auth
    class UserStore
      include Enumerable({String, User})
      private DIRECT_USER = "__direct"
      Log         = LavinMQ::Log.for "user_store"

      def self.hidden?(name)
        DIRECT_USER == name
      end

      @save_lock = Mutex.new

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        # Global users, keyed by name
        @users = Hash(String, User).new
        # Vhost scoped users, keyed by vhost and then name. A scoped user's name
        # only has to be unique within its vhost.
        @vhost_users = Hash(String, Hash(String, User)).new
        load!
      end

      # Look up a user. Without `vhost` only global users are considered,
      # with `vhost` only users scoped to that vhost.
      def []?(name : String, vhost : String? = nil) : User?
        if vhost
          @vhost_users[vhost]?.try &.[name]?
        else
          @users[name]?
        end
      end

      def [](name : String, vhost : String? = nil) : User
        self[name, vhost]? || raise KeyError.new("Missing user: #{vhost ? "#{vhost}:" : ""}#{name}")
      end

      # Find the user `name` for a login to `vhost`: a user scoped to the vhost
      # takes precedence over a global user with the same name.
      def find(name : String, vhost : String) : User?
        self[name, vhost]? || @users[name]?
      end

      # Users scoped to `vhost`
      def vhost_users(vhost : String) : Array(User)
        @vhost_users[vhost]?.try(&.values) || Array(User).new(0)
      end

      def each_value(& : User ->) : Nil
        @users.each_value { |u| yield u }
        @vhost_users.each_value &.each_value { |u| yield u }
      end

      def size : Int32
        @users.size + @vhost_users.sum(0) { |_, users| users.size }
      end

      def values : Array(User)
        arr = Array(User).new(size)
        each_value { |u| arr << u }
        arr
      end

      def each(&)
        @users.each do |kv|
          yield kv
        end
        @vhost_users.each_value &.each do |kv|
          yield kv
        end
      end

      # Adds a user to the use store
      def create(name, password, tags = Array(Tag).new, save = true, vhost : String? = nil)
        if user = self[name, vhost]?
          return user
        end
        user = User.create(name, password, "SHA256", tags, vhost)
        store(user)
        Log.info { "Created user=#{user.login_name}" }
        save! if save
        user
      end

      def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true, vhost : String? = nil)
        user = User.new(name, password_hash, password_algorithm, tags, vhost)
        store(user)
        save! if save
        user
      end

      private def store(user : User)
        if vhost = user.vhost
          (@vhost_users[vhost] ||= Hash(String, User).new)[user.name] = user
        else
          @users[user.name] = user
        end
      end

      def add_permission(user : User, vhost, config, read, write, save = true)
        if (user_vhost = user.vhost) && user_vhost != vhost
          raise VHostScopeError.new("User '#{user.login_name}' is scoped to vhost '#{user_vhost}' and can't have permissions on vhost '#{vhost}'")
        end
        perm = {config: config, read: read, write: write}
        if user.permissions[vhost]? == perm
          return perm
        end
        user.permissions[vhost] = perm
        user.clear_permissions_cache
        save! if save
        perm
      end

      # Adds permissions for the global user `user`
      def add_permission(user : String, vhost, config, read, write, save = true)
        add_permission(@users[user], vhost, config, read, write, save)
      end

      def rm_permission(user : User, vhost)
        if perm = user.permissions.delete vhost
          user.clear_permissions_cache
          Log.info { "Removed permissions for user=#{user.login_name} on vhost=#{vhost}" }
          save!
          perm
        end
      end

      def rm_permission(user : String, vhost)
        rm_permission(@users[user], vhost)
      end

      # Called when a vhost is deleted: removes all permissions on the vhost
      # and all users scoped to it
      def rm_vhost_permissions_for_all(vhost)
        @users.each_value do |user|
          user.permissions.delete(vhost)
          user.clear_permissions_cache
        end
        if scoped = @vhost_users.delete(vhost)
          scoped.each_value do |user|
            user.permissions.clear
            user.clear_permissions_cache
            Log.info { "Deleted user=#{user.login_name}" }
          end
        end
        save!
      end

      def delete(name, save = true, vhost : String? = nil) : User?
        return if name == DIRECT_USER
        user = if vhost
                 if scoped = @vhost_users[vhost]?
                   u = scoped.delete name
                   @vhost_users.delete(vhost) if scoped.empty?
                   u
                 end
               else
                 @users.delete name
               end
        if user
          user.permissions.clear
          user.clear_permissions_cache
          Log.info { "Deleted user=#{user.login_name}" }
          save! if save
          user
        end
      end

      def default_user : User
        @users.each_value do |u|
          if u.tags.includes?(Tag::Administrator) && !u.hidden?
            return u
          end
        end
        @users.each_value do |u|
          if u.tags.includes?(Tag::Administrator)
            return u
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

      def direct_user
        @users[DIRECT_USER]
      end

      private def load!
        path = File.join(@data_dir, "users.json")
        if File.exists? path
          Log.debug { "Loading users from file" }
          File.open(path) do |f|
            Array(User).from_json(f) do |user|
              store(user)
            end
            @replicator.try &.register_file f
          end
        elsif Config.instance.load_definitions.empty?
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
        @users[DIRECT_USER] = User.create_hidden_user(DIRECT_USER)
        perm = {config: /.*/, read: /.*/, write: /.*/}
        @users[DIRECT_USER].permissions["/"] = perm
      end

      def save!
        Log.debug { "Saving users to file" }
        path = File.join(@data_dir, "users.json")
        tmpfile = "#{path}.tmp"
        # Serialize saves so concurrent user add/delete don't race on the shared
        # `.tmp` file and fail the rename.
        @save_lock.synchronize do
          File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
          File.rename tmpfile, path
        end
        @replicator.try &.replace_file path
      end

      class VHostScopeError < ArgumentError; end
    end
  end
end
