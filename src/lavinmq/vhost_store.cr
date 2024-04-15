require "json"
require "./vhost"
require "./user"

module LavinMQ
  class VHostStore
    include Enumerable({String, VHost})
    Log = ::Log.for "vhoststore"

    def initialize(@data_dir : String, @users : UserStore, @replicator : Replication::Replicator)
      @vhosts = Hash(String, VHost).new
      load!
    end

    forward_missing_to @vhosts

    def each(&)
      @vhosts.each do |kv|
        yield kv
      end
    end

    def create(name : String, user : User = @users.default_user, save : Bool = true)
      if v = @vhosts[name]?
        return v
      end
      vhost = VHost.new(name, @data_dir, @users, @replicator)
      Log.info { "Created vhost #{name}" }
      @users.add_permission(user.name, name, /.*/, /.*/, /.*/)
      @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
      @vhosts[name] = vhost
      save! if save
      vhost
    end

    def delete(name) : Nil
      if vhost = @vhosts.delete name
        Log.info { "Deleted vhost #{name}" }
        @users.rm_vhost_permissions_for_all(name)
        vhost.delete
        save!
      end
    end

    def close
      @vhosts.each_value &.close
    end

    def to_json(json : JSON::Builder)
      json.array do
        @vhosts.each_value do |vhost|
          vhost.to_json(json)
        end
      end
    end

    private def load!
      path = File.join(@data_dir, "vhosts.json")
      if File.exists? path
        Log.debug { "Loading vhosts from file" }
        File.open(path) do |f|
          JSON.parse(f).as_a.each do |vhost|
            name = vhost["name"].as_s
            @vhosts[name] = VHost.new(name, @data_dir, @users, @replicator)
            @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
          end
          @replicator.register_file(f)
        end
      else
        Log.debug { "Loading default vhosts" }
        create("/")
      end
      Log.debug { "#{size} vhosts loaded" }
    end

    private def save!
      Log.debug { "Saving vhosts to file" }
      path = File.join(@data_dir, "vhosts.json")
      File.open("#{path}.tmp", "w") { |f| to_pretty_json(f); f.fsync }
      File.rename "#{path}.tmp", path
      @replicator.replace_file path
    end
  end
end
