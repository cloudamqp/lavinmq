require "json"
require "./user"

module AvalancheMQ
  class UserStore
    include Enumerable({String, User})

    def initialize(@data_dir : String, @log : Logger)
      @users = Hash(String, User).new
      load!
    end

    forward_missing_to @users

    def each
      @users.each do |kv|
        yield kv
      end
    end

    # Adds a user to the use store
    def create(name, password, tags = Array(Tag).new, save = true)
      if user = @users[name]?
        return user
      end
      user = User.create(name, password, "Bcrypt", tags)
      @users[name] = user
      save! if save
      user
    end

    def add(name, password_hash, password_algorithm, tags = Array(Tag).new, save = true)
      return if has_key?(name)
      user = User.new(name, password_hash, password_algorithm, tags)
      @users[name] = user
      save! if save
      user
    end

    def add_permission(user, vhost, config, read, write)
      perm = {config: config, read: read, write: write}
      @users[user].permissions[vhost] = perm
      @users[user].invalidate_acl_caches
      save!
      perm
    end

    def rm_permission(user, vhost)
      if perm = @users[user].permissions.delete vhost
        @users[user].invalidate_acl_caches
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
      if user = @users.delete name
        save! if save
        user
      end
    end

    def default_user
      tu = @users.find { |_, u| u.tags.includes? Tag::Administrator }
      raise "No user with administrator privileges found" if tu.nil?
      tu.not_nil!.last
    end

    def to_json(json : JSON::Builder)
      json.array do
        each_value do |user|
          user.to_json(json)
        end
      end
    end

    private def load!
      path = File.join(@data_dir, "users.json")
      if File.exists? path
        @log.debug "Loading users from file"
        File.open(path) do |f|
          Array(User).from_json(f) do |user|
            @users[user.name] = user
          end
        rescue JSON::ParseException
          @log.warn("#{path} is not vaild json")
        end
      else
        tags = [Tag::Administrator]
        @log.debug "Loading default users"
        create("guest", "guest", tags, save: false)
        add_permission("guest", "/", /.*/, /.*/, /.*/)
        save!
      end
      @log.debug("#{size} users loaded")
    end

    def save!
      @log.debug "Saving users to file"
      tmpfile = File.join(@data_dir, "users.json.tmp")
      File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
      File.rename tmpfile, File.join(@data_dir, "users.json")
    end
  end
end
