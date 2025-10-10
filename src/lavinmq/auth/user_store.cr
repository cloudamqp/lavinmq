require "json"
require "./user"
require "./temp_user"

module LavinMQ
  module Auth
    class UserStore
      include Enumerable({String, User})
      DIRECT_USER = "__direct"
      Log         = LavinMQ::Log.for "user_store"

      def self.hidden?(name)
        DIRECT_USER == name
      end

      def initialize(@data_dir : String, @replicator : Clustering::Replicator)
        @users = Hash(String, User).new
        @temp_users = Hash(String, TempUser).new
        load!
        spawn cleanup_expired_users
      end

      forward_missing_to @users

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

      def add(name : String, tags : Array(Tag), permissions : Hash(String, User::Permissions), expires_at : Time)
        if cached = @temp_users[name]?
          return cached unless cached.expired?
          @temp_users.delete(name)
          Log.debug { "Removed expired temp user=#{name}" }
        end
        user = TempUser.new(name, tags, permissions, expires_at)
        @temp_users[user.name] = user
        Log.info { "Added temp user=#{user.name}" }
        user
      end

      def temp_user_count
        @temp_users.size
      end

      def each_temp_user
        @temp_users.each_value.reject(&.expired?)
      end

      # override forward_missing_to and check for all types of users
      def []?(name : String) : User?
        if user = @users[name]?
          return user
        end
        if temp_user = @temp_users[name]?
          if temp_user.expired?
            @temp_users.delete(name)
            Log.debug { "Removed expired temp user=#{name}" }
            return nil
          end
          return temp_user
        end
        nil
      end

      def add_permission(user, vhost, config, read, write)
        perm = {config: config, read: read, write: write}
        if @users[user].permissions[vhost]? && @users[user].permissions[vhost] == perm
          return perm
        end
        @users[user].permissions[vhost] = perm
        save!
        perm
      end

      def rm_permission(user, vhost)
        if perm = @users[user].permissions.delete vhost
          Log.info { "Removed permissions for user=#{user} on vhost=#{vhost}" }
          save!
          perm
        end
      end

      def rm_vhost_permissions_for_all(vhost)
        @users.each_value do |user|
          user.permissions.delete(vhost)
        end
        save!
      end

      def delete(name, save = true) : User?
        return if name == DIRECT_USER
        if user = @temp_users[name]?
          @temp_users.delete(name)
          Log.info { "Deleted temp user=#{name}" }
          return user
        end
        if user = @users.delete name
          Log.info { "Deleted user=#{name}" }
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
              @users[user.name] = user
            end
            @replicator.register_file f
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

      private def cleanup_expired_users
        loop do
          sleep 60.seconds
          expired = @temp_users.select { |_, u| u.expired? }
          expired.each_key { |name| @temp_users.delete(name) }
          Log.debug { "Cleaned up #{expired.size} expired temp users" } if expired.any?
        end
      end

      def save!
        Log.debug { "Saving users to file" }
        path = File.join(@data_dir, "users.json")
        tmpfile = "#{path}.tmp"
        File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
        File.rename tmpfile, path
        @replicator.replace_file path
      end
    end
  end
end
