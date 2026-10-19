require "../filesystem"
require "json"
require "./user"
require "./internal_user"

module LavinMQ
  module Auth
    class UserStore
      include Enumerable({String, User})
      Log = LavinMQ::Log.for "user_store"

      # Names reserved for the broker's own identities. "__direct" was the
      # password-based user earlier versions used for shovels; it stays
      # reserved so it can't be recreated as a regular login.
      RESERVED_NAMES = {InternalUser::NAME, "__direct"}

      def self.hidden?(name)
        RESERVED_NAMES.includes?(name)
      end

      # The passwordless identity of the broker itself, see InternalUser.
      getter internal_user = InternalUser.new

      @save_lock = Mutex.new

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @users = Hash(String, User).new
        load!
      end

      def []?(name : String) : User?
        @users[name]?
      end

      def [](name : String) : User
        @users[name]
      end

      def each_value(& : User ->) : Nil
        @users.each_value { |u| yield u }
      end

      def size : Int32
        @users.size
      end

      def values : Array(User)
        @users.values
      end

      def each(&)
        @users.each do |kv|
          yield kv
        end
      end

      # Adds a user to the use store
      def create(name, password, tags = Array(Tag).new, save = true)
        if user = @users[name]?
          return user
        end
        user = User.create(name, password, "SHA256", tags)
        @users[name] = user
        Log.info { "Created user=#{name}" }
        save! if save
        user
      end

      def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true)
        user = User.new(name, password_hash, password_algorithm, tags)
        @users[name] = user
        save! if save
        user
      end

      def add_permission(user : User, vhost, config, read, write, save = true)
        add_permission(user.name, vhost, config, read, write, save)
      end

      def add_permission(user, vhost, config, read, write, save = true)
        perm = {config: config, read: read, write: write}
        if @users[user].permissions[vhost]? && @users[user].permissions[vhost] == perm
          return perm
        end
        @users[user].permissions[vhost] = perm
        @users[user].clear_permissions_cache
        save! if save
        perm
      end

      def rm_permission(user, vhost)
        if perm = @users[user].permissions.delete vhost
          @users[user].clear_permissions_cache
          Log.info { "Removed permissions for user=#{user} on vhost=#{vhost}" }
          save!
          perm
        end
      end

      def rm_vhost_permissions_for_all(vhost)
        @users.each_value do |user|
          user.permissions.delete(vhost)
          user.clear_permissions_cache
        end
        save!
      end

      def delete(name, save = true) : User?
        return if self.class.hidden?(name)
        if user = @users.delete name
          user.permissions.clear
          user.clear_permissions_cache
          Log.info { "Deleted user=#{name}" }
          save! if save
          user
        end
      end

      # The administrator new vhosts are granted to when no user is given.
      # Falls back to the internal user when there is no administrator.
      def default_user : BaseUser
        @users.each_value do |u|
          if u.tags.includes?(Tag::Administrator) && !u.hidden?
            return u
          end
        end
        internal_user
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
            Array(User).from_json(f) do |user|
              @users[user.name] = user
            end
            @replicator.try &.register_file f
          end
        elsif Config.instance.load_definitions.empty?
          Log.debug { "Loading default users" }
          create_default_user
        end
        drop_reserved_users
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

      # Reserved names are never logins, even if a users.json (or an older
      # definitions import) carries one.
      private def drop_reserved_users
        RESERVED_NAMES.each { |name| @users.delete(name) }
      end

      def save!
        Log.debug { "Saving users to file" }
        path = File.join(@data_dir, "users.json")
        # Serialize saves so concurrent user add/delete don't race on the shared
        # `.tmp` file and fail the rename.
        @save_lock.synchronize do
          FileSystem.replace(path) { |f| to_pretty_json(f) }
        end
        @replicator.try &.replace_file path
      end
    end
  end
end
