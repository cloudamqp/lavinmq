require "json"
require "./user"

module AvalancheMQ
  class UserStore
    def initialize(@data_dir : String, @log : Logger)
      @users = Hash(String, User).new
      load!
    end

    def [](name)
      @users[name]
    end

    def []?(name)
      @users[name]?
    end

    def size
      @users.size
    end

    # Adds a user to the use store
    # Returns nil if user is already created
    def create(name, password, save = true)
      return if @users.has_key?(name)
      user = User.create(name, password, "Bcrypt")
      user.permissions["/"] = { config: /.*/, read: /.*/, write: /.*/ }
      @users[name] = user
      save! if save
      user
    end

    def add(name, password_hash, password_algorithm, save = true)
      return if @users.has_key?(name)
      user = User.new(name, password_hash, password_algorithm)
      user.permissions["/"] = { config: /.*/, read: /.*/, write: /.*/ }
      @users[name] = user
      save! if save
      user
    end

    def add_permission(user, vhost, config, read, write)
      perm = { config: config, read: read, write: write }
      @users[user].permissions[vhost] = perm
      save!
      perm
    end

    def rm_permission(user, vhost)
      if perm = @users[user].permissions.delete vhost
        save!
        perm
      end
    end

    def delete(name) : User?
      if user = @users.delete name
        save!
        user
      end
    end

    def to_json(json : JSON::Builder)
      @users.values.to_json(json)
    end

    private def load!
      path = File.join(@data_dir, "users.json")
      if File.exists? path
        @log.debug "Loading users from file"
        File.open(path) do |f|
          Array(User).from_json(f) do |user|
            @users[user.name] = user
          end
        end
      else
        @log.debug "Loading default users"
        create("guest", "guest", save: false)
        save!
      end
      @log.debug("#{@users.size} users loaded")
    end

    def save!
      @log.debug "Saving users to file"
      tmpfile = File.join(@data_dir, "users.json.tmp")
      File.open(tmpfile, "w") { |f| self.to_json(f) }
      File.rename tmpfile, File.join(@data_dir, "users.json")
    end
  end
end
