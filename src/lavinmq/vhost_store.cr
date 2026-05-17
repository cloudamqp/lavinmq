require "json"
require "sync/shared"
require "./vhost"
require "./auth/base_user"
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

    @vhosts : Sync::Shared(Hash(String, VHost))
    # Serializes save! so concurrent admin operations don't both
    # truncate-and-rewrite vhosts.json.tmp from offset zero.
    @save_lock = Mutex.new

    def initialize(@data_dir : String, @users : Auth::UserStore, @replicator : Clustering::Replicator?)
      @vhosts = Sync::Shared.new(Hash(String, VHost).new, :unchecked)
    end

    def []?(name : String) : VHost?
      @vhosts.shared { |v| v[name]? }
    end

    def [](name : String) : VHost
      @vhosts.shared { |v| v[name] }
    end

    def each_value(& : VHost ->) : Nil
      @vhosts.shared do |vhosts|
        vhosts.each_value { |v| yield v }
      end
    end

    def has_key?(name : String) : Bool
      @vhosts.shared(&.has_key?(name))
    end

    def size : Int32
      @vhosts.unsafe_get.size
    end

    def values : Array(VHost)
      @vhosts.shared(&.values)
    end

    def first_value : VHost
      @vhosts.shared(&.first_value)
    end

    def first_value? : VHost?
      @vhosts.shared(&.first_value?)
    end

    def each(&)
      @vhosts.shared do |vhosts|
        vhosts.each do |kv|
          yield kv
        end
      end
    end

    def create(name : String, user : Auth::BaseUser = @users.default_user, description = "", tags = Array(String).new(0), save : Bool = true)
      created = false
      vhost = @vhosts.lock do |vh|
        if v = vh[name]?
          next v
        end
        new_vhost = VHost.new(name, @data_dir, @users, @replicator, description, tags)
        Log.info { "Created vhost #{name}" }
        @users.add_permission(user.name, name, /.*/, /.*/, /.*/, save: false)
        @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/, save: false)
        vh[name] = new_vhost
        created = true
        new_vhost
      end
      if created
        @users.save!
        save! if save
        notify_observers(Event::Added, name)
      end
      vhost
    end

    def delete(name) : VHost?
      if vhost = @vhosts.lock(&.delete(name))
        Log.info { "Deleting vhost #{name}" }
        @users.rm_vhost_permissions_for_all(name)
        vhost.delete
        notify_observers(Event::Deleted, name)
        Log.info { "Deleted vhost #{name}" }
        save!
        vhost
      end
    end

    def close
      WaitGroup.wait do |wg|
        @vhosts.shared(&.values).each do |vhost|
          wg.spawn do
            vhost.close
            notify_observers(Event::Closed, vhost.name)
          end
        end
      end
    end

    def to_json(json : JSON::Builder)
      json.array do
        each_value do |vhost|
          vhost.to_json(json)
        end
      end
    end

    def load!
      path = File.join(@data_dir, "vhosts.json")
      begin
        File.open(path) do |f|
          Log.debug { "Loading vhosts.json" }
          @vhosts.lock do |vhosts|
            JSON.parse(f).as_a.each do |vhost|
              name = vhost["name"].as_s
              tags = vhost["tags"]?.try(&.as_a.map(&.to_s)) || [] of String
              description = vhost["description"]?.try &.as_s || ""
              vhosts[name] = VHost.new(name, @data_dir, @users, @replicator, description, tags)
              @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/)
            end
          end
          @replicator.try &.register_file(f)
        end
      rescue File::NotFoundError
        if Config.instance.load_definitions.empty?
          Log.debug { "Loading default vhosts" }
          create("/")
        end
      end
      # Wait for vhosts to be loaded ("not closed")
      @vhosts.shared(&.values).each &.closed.when_false.receive
      Log.debug { "#{size} vhosts loaded" }
    rescue ex
      Log.error(exception: ex) { "Failed to load vhosts" }
      raise ex
    end

    private def save!
      @save_lock.synchronize do
        Log.debug { "Saving vhosts to file" }
        path = File.join(@data_dir, "vhosts.json")
        File.open("#{path}.tmp", "w") { |f| to_pretty_json(f); f.fsync }
        File.rename "#{path}.tmp", path
        @replicator.try &.replace_file path
      end
    end
  end
end
