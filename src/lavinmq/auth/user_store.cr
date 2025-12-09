require "json"
require "./user"

module LavinMQ
  module Auth
    class UserStore
      include Enumerable({String, User})
      DIRECT_USER = "__direct"
      Log         = LavinMQ::Log.for "user_store"

      def self.hidden?(name)
        DIRECT_USER == name
      end

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @users = Hash(String, User).new
        @lock = Mutex.new(:reentrant)
        load!
      end

      def []?(key)
        @lock.synchronize { @users[key]? }
      end

      def [](key)
        @lock.synchronize { @users[key] }
      end

      def has_key?(key)
        @lock.synchronize { @users.has_key?(key) }
      end

      def size
        @lock.synchronize { @users.size }
      end

      def each(&)
        @lock.synchronize do
          @users.each do |kv|
            yield kv
          end
        end
      end

      def each_value(&)
        @lock.synchronize do
          @users.each_value do |v|
            yield v
          end
        end
      end

      def each_value : Iterator(User)
        # Hash#values returns a new array, safe to iterate outside lock
        @lock.synchronize do
          @users.values.each
        end
      end

      # Adds a user to the use store
      def create(name, password, tags = Array(Tag).new, save = true)
        @lock.synchronize do
          if user = @users[name]?
            return user
          end
          user = User.create(name, password, "SHA256", tags)
          @users[name] = user
          Log.info { "Created user=#{name}" }
          save! if save
          user
        end
      end

      def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true)
        @lock.synchronize do
          user = User.new(name, password_hash, password_algorithm, tags)
          @users[name] = user
          save! if save
          user
        end
      end

      def add_permission(user, vhost, config, read, write)
        @lock.synchronize do
          perm = {config: config, read: read, write: write}
          if @users[user].permissions[vhost]? && @users[user].permissions[vhost] == perm
            return perm
          end
          @users[user].permissions[vhost] = perm
          @users[user].clear_permissions_cache
          save!
          perm
        end
      end

      def rm_permission(user, vhost)
        @lock.synchronize do
          if perm = @users[user].permissions.delete vhost
            @users[user].clear_permissions_cache
            Log.info { "Removed permissions for user=#{user} on vhost=#{vhost}" }
            save!
            perm
          end
        end
      end

      def rm_vhost_permissions_for_all(vhost)
        @lock.synchronize do
          @users.each_value do |user|
            user.permissions.delete(vhost)
            user.clear_permissions_cache
          end
          save!
        end
      end

      def delete(name, save = true) : User?
        return if name == DIRECT_USER
        @lock.synchronize do
          if user = @users.delete name
            user.permissions.clear
            user.clear_permissions_cache
            Log.info { "Deleted user=#{name}" }
            save! if save
            user
          end
        end
      end

      def default_user : User
        @lock.synchronize do
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
              @users[user.name] = user
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
        add(Config.instance.default_user, Config.instance.default_password, "SHA256", tags: [Tag::Administrator], save: false)
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
        File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
        File.rename tmpfile, path
        @replicator.try &.replace_file path
      end
    end
  end
end
