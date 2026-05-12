require "json"
require "sync/shared"
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

      @users : Sync::Shared(Hash(String, User))
      # Serializes save! so concurrent admin operations don't both
      # truncate-and-rewrite users.json.tmp from offset zero.
      @save_lock = Mutex.new

      def initialize(@data_dir : String, @replicator : Clustering::Replicator?)
        @users = Sync::Shared.new(Hash(String, User).new, :unchecked)
        load!
      end

      def []?(name : String) : User?
        @users.shared { |u| u[name]? }
      end

      def [](name : String) : User
        @users.shared { |u| u[name] }
      end

      def each_value(& : User ->) : Nil
        @users.shared do |users|
          users.each_value { |u| yield u }
        end
      end

      def has_key?(name : String) : Bool
        @users.shared(&.has_key?(name))
      end

      def size : Int32
        @users.unsafe_get.size
      end

      def values : Array(User)
        @users.shared(&.values)
      end

      def each(&)
        @users.shared do |users|
          users.each do |kv|
            yield kv
          end
        end
      end

      # Adds a user to the use store
      def create(name, password, tags = Array(Tag).new, save = true)
        created = false
        user = @users.lock do |users|
          if existing = users[name]?
            next existing
          end
          new_user = User.create(name, password, "SHA256", tags)
          users[name] = new_user
          Log.info { "Created user=#{name}" }
          created = true
          new_user
        end
        save! if created && save
        user
      end

      def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true)
        user = User.new(name, password_hash, password_algorithm, tags)
        @users.lock { |u| u[name] = user }
        save! if save
        user
      end

      def add_permission(user : User, vhost, config, read, write, save = true)
        add_permission(user.name, vhost, config, read, write, save: save)
      end

      def add_permission(user, vhost, config, read, write, save = true)
        perm = {config: config, read: read, write: write}
        changed = @users.lock do |users|
          users[user].set_permission(vhost, perm)
        end
        save! if changed && save
        perm
      end

      def rm_permission(user, vhost)
        perm = @users.lock do |users|
          users[user].delete_permission(vhost)
        end
        if perm
          Log.info { "Removed permissions for user=#{user} on vhost=#{vhost}" }
          save!
        end
        perm
      end

      def rm_vhost_permissions_for_all(vhost)
        @users.lock do |users|
          users.each_value do |user|
            user.delete_permission(vhost)
          end
        end
        save!
      end

      def delete(name, save = true) : User?
        return if name == DIRECT_USER
        user = @users.lock do |users|
          next nil unless u = users.delete(name)
          u.clear_permissions
          u
        end
        if user
          Log.info { "Deleted user=#{name}" }
          save! if save
          user
        end
      end

      def default_user : User
        @users.shared do |users|
          users.each_value do |u|
            if u.tags.includes?(Tag::Administrator) && !u.hidden?
              return u
            end
          end
          users.each_value do |u|
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

      def direct_user
        @users.shared { |u| u[DIRECT_USER] }
      end

      private def load!
        path = File.join(@data_dir, "users.json")
        if File.exists? path
          Log.debug { "Loading users from file" }
          File.open(path) do |f|
            @users.lock do |users|
              Array(User).from_json(f) do |user|
                users[user.name] = user
              end
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
        add_permission(Config.instance.default_user, "/", /.*/, /.*/, /.*/, save: false)
        save!
      end

      private def create_direct_user
        direct = User.create_hidden_user(DIRECT_USER)
        @users.lock { |u| u[DIRECT_USER] = direct }
        perm = {config: /.*/, read: /.*/, write: /.*/}
        direct.set_permission("/", perm)
      end

      def save!
        @save_lock.synchronize do
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
end
