require "json"
require "./vhost"
require "./auth/user"
require "./observable"

module LavinMQ
  class VHostStore
    enum Event
      Added
      Deleted
      Closed
    end
    include Enumerable({String, VHost})
    include Observable(Event)

    Log = LavinMQ::Log.for "vhost_store"

    def initialize(@data_dir : String, @users : UserStore, @replicator : Clustering::Replicator)
      @vhosts = Hash(String, VHost).new
      @lock = Mutex.new
      load!
    end

    def each(&)
      @vhosts.each do |kv|
        yield kv
      end
    end

    def each_value(&)
      @vhosts.each_value do |vhost|
        yield vhost
      end
    end

    def first_value
      @vhosts.first_value
    end

    def each_value
      @vhosts.each_value
    end

    def []?(name : String) : VHost?
      @vhosts[name]?
    end

    def [](name : String) : VHost
      @vhosts[name]
    end

    def capacity
      @vhosts.capacity
    end

    def create(name : String, user : User = @users.default_user, description = "", tags = Array(String).new(0), save : Bool = true)
      @lock.synchronize do
        if v = @vhosts[name]?
          return v
        end
        vhost = VHost.new(name, @data_dir, @users, @replicator, description, tags)
        Log.info { "Created vhost #{name}" }
        @users.add_permission(user.name, name, /.*/, /.*/, /.*/)
        @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
        new_vhosts = @vhosts.dup
        new_vhosts[name] = vhost
        @vhosts = new_vhosts
        save! if save
        notify_observers(Event::Added, name)
        vhost
      end
    end

    def delete(name) : VHost?
      @lock.synchronize do
        if vhost = @vhosts[name]?
          @users.rm_vhost_permissions_for_all(name)
          vhost.delete
          new_vhosts = @vhosts.dup
          new_vhosts.delete(name)
          @vhosts = new_vhosts
          notify_observers(Event::Deleted, name)
          Log.info { "Deleted vhost #{name}" }
          save!
          vhost
        end
      end
    end

    def close
      WaitGroup.wait do |wg|
        @vhosts.each_value do |vhost|
          wg.spawn do
            vhost.close
            notify_observers(Event::Closed, vhost.name)
          end
        end
      end
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
            tags = vhost["tags"]?.try(&.as_a.map(&.to_s)) || [] of String
            description = vhost["description"]?.try &.as_s || ""
            @vhosts[name] = VHost.new(name, @data_dir, @users, @replicator, description, tags)
            @users.add_permission(UserStore::DIRECT_USER, name, /.*/, /.*/, /.*/)
          end
          @replicator.register_file(f)
        end
      else
        Log.debug { "Loading default vhosts" }
        create("/")
      end
      Log.debug { "#{size} vhosts loaded" }
    rescue ex
      Log.error(exception: ex) { "Failed to load vhosts" }
      raise ex
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
