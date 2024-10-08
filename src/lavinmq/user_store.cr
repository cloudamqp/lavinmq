require "json"
require "./user"

module LavinMQ
  class UserStore
    include Enumerable({String, User})
    DIRECT_USER = "__direct"
    Log         = LavinMQ::Log.for "user_store"

    def self.hidden?(name)
      DIRECT_USER == name
    end

    def initialize(@data_dir : String, @replicator : Clustering::Replicator)
      @users = Hash(String, User).new
      load!
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

    def add_permission(user, vhost, config, read, write)
      perm = {config: config, read: read, write: write}
      if @users[user].permissions[vhost]? && @users[user].permissions[vhost] == perm
        return perm
      end
      @users[user].permissions[vhost] = perm
      @users[user].invalidate_acl_caches
      save!
      perm
    end

    def rm_permission(user, vhost)
      if perm = @users[user].permissions.delete vhost
        @users[user].invalidate_acl_caches
        Log.info { "Removed permissions for user=#{user} on vhost=#{vhost}" }
        save!
        perm
      end
    end

    def rm_vhost_permissions_for_all(vhost)
      @users.each_value do |user|
        if user.permissions.delete(vhost)
          user.invalidate_acl_caches
        end
      end
      save!
    end

    def delete(name, save = true) : User?
      return if name == DIRECT_USER
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
        tags = [Tag::Administrator]
        Log.debug { "Loading default users" }
        create("guest", "guest", tags, save: false)
        add_permission("guest", "/", /.*/, /.*/, /.*/)
        save!
      end
      create_direct_user
      Log.debug { "#{size} users loaded" }
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
      @replicator.replace_file path
    end
  end
end
