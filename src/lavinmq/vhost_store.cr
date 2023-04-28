require "json"
require "./vhost"
require "./user"

module LavinMQ
  class VHostStore
    include Enumerable({String, VHost})
    @log = Log.for "vhoststore"

    def initialize(@data_dir : String, @users : UserStore, @replicator : Replication::Server)
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
      @log.info { "vhost=#{name} created" }
      @users.add_permission(user.name, name, /.*/, /.*/, /.*/)
      @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
      @vhosts[name] = vhost
      save! if save
      vhost
    end

    def delete(name) : Nil
      if vhost = @vhosts.delete name
        @log.info { "Deleting vhost=#{name}" }
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
        @log.debug { "Loading vhosts from file" }
        File.open(path) do |f|
          JSON.parse(f).as_a.each do |vhost|
            next unless vhost.as_h?
            name = vhost["name"].as_s
            @vhosts[name] = VHost.new(name, @data_dir, @users, @replicator)
            @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
          end
        rescue JSON::ParseException
          @log.warn { "#{path} is not valid json" }
        end
        @replicator.add_file(path)
      else
        @log.debug { "Loading default vhosts" }
        create("/")
      end
      @log.debug { "#{size} vhosts loaded" }
    end

    private def save!
      @log.debug { "Saving vhosts to file" }
      path = File.join(@data_dir, "vhosts.json")
      File.open("#{path}.tmp", "w") { |f| to_pretty_json(f); f.fsync }
      File.rename "#{path}.tmp", path
      @replicator.add_file path
    end
  end
end
