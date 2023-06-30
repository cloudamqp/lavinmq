require "json"
require "./vhost"
require "./user"

module LavinMQ
  class VHostStore
    include Enumerable({String, VHost})
    @log = Log.for "vhoststore"
    @lock = Mutex.new(:reentrant) # required for save! -> to_json

    def initialize(@data_dir : String, @users : UserStore, @replicator : Replication::Server)
      @vhosts = Hash(String, VHost).new
      load!
    end

    def create(name : String, created_by : User = @users.default_user, save : Bool = true) : VHost
      @lock.synchronize do
        if v = @vhosts[name]?
          return v
        end
        vhost = VHost.new(name, @data_dir, @users, @replicator)
        @vhosts[name] = vhost
        @users.add_permission(created_by.name, name, /.*/, /.*/, /.*/)
        @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
        @log.info { "vhost #{name} created, by #{created_by.name}" }
        save! if save
        vhost
      end
    end

    def delete(name) : Nil
      @lock.synchronize do
        if vhost = @vhosts.delete name
          @log.info { "Deleting vhost=#{name}" }
          @users.rm_vhost_permissions_for_all(name)
          vhost.delete
          save!
        end
      end
    end

    def close
      @lock.synchronize do
        @vhosts.each_value &.close
        @vhosts.clear
      end
    end

    def to_json(json : JSON::Builder)
      json.array do
        each_value &.to_json(json)
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

    def each(&)
      @lock.synchronize do
        @vhosts.each do |kv|
          yield kv
        end
      end
    end

    def each_value(&)
      @lock.synchronize do
        @vhosts.each_value do |kv|
          yield kv
        end
      end
    end

    def each_key(&)
      @lock.synchronize do
        @vhosts.each_key do |kv|
          yield kv
        end
      end
    end

    def each_value
      @vhosts.each_value
    end

    def []?(key)
      @lock.synchronize do
        @vhosts[key]?
      end
    end

    def [](key)
      @lock.synchronize do
        @vhosts[key]
      end
    end

    def capacity
      @vhosts.capacity
    end

    def first_value
      @lock.synchronize do
        @vhosts.first_value
      end
    end
  end
end
