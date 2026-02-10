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

    def initialize(@data_dir : String, @users : Auth::UserStore, @replicator : Clustering::Replicator?)
      @vhosts = Sync::Shared.new(Hash(String, VHost).new)
      load!
    end

    # Explicit accessors replacing forward_missing_to

    def []?(name : String) : VHost?
      @vhosts.shared { |h| h[name]? }
    end

    def [](name : String) : VHost
      @vhosts.shared { |h| h[name] }
    end

    def each_value(& : VHost ->)
      @vhosts.shared { |h| h.each_value { |v| yield v } }
    end

    def each_value : Iterator(VHost)
      @vhosts.shared(&.values).each
    end

    def has_key?(name : String) : Bool
      @vhosts.shared(&.has_key?(name))
    end

    def size : Int32
      @vhosts.shared(&.size)
    end

    def values : Array(VHost)
      @vhosts.shared(&.values)
    end

    def first_value : VHost
      @vhosts.shared(&.first_value)
    end

    def each(&)
      @vhosts.shared do |h|
        h.each do |kv|
          yield kv
        end
      end
    end

    def create(name : String, user : Auth::BaseUser = @users.default_user, description = "", tags = Array(String).new(0), save : Bool = true)
      created = false
      vhost = @vhosts.lock do |h|
        if v = h[name]?
          next v
        end
        v = VHost.new(name, @data_dir, @users, @replicator, description, tags)
        h[name] = v
        created = true
        v
      end
      if created
        Log.info { "Created vhost #{name}" }
        @users.add_permission(user.name, name, /.*/, /.*/, /.*/)
        @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/)
        save! if save
        notify_observers(Event::Added, name)
      end
      vhost
    end

    def delete(name) : VHost?
      vhost = @vhosts.lock(&.delete(name))
      if vhost
        @users.rm_vhost_permissions_for_all(name)
        vhost.delete
        notify_observers(Event::Deleted, name)
        Log.info { "Deleted vhost #{name}" }
        save!
        vhost
      end
    end

    def close
      vhosts = @vhosts.shared(&.values)
      WaitGroup.wait do |wg|
        vhosts.each do |vhost|
          wg.spawn do
            vhost.close
            notify_observers(Event::Closed, vhost.name)
          end
        end
      end
    end

    def to_json(json : JSON::Builder)
      json.array do
        @vhosts.shared do |h|
          h.each_value do |vhost|
            vhost.to_json(json)
          end
        end
      end
    end

    private def load!
      path = File.join(@data_dir, "vhosts.json")
      if File.exists? path
        Log.debug { "Loading vhosts from file" }
        File.open(path) do |f|
          @vhosts.lock do |h|
            JSON.parse(f).as_a.each do |vhost|
              name = vhost["name"].as_s
              tags = vhost["tags"]?.try(&.as_a.map(&.to_s)) || [] of String
              description = vhost["description"]?.try &.as_s || ""
              h[name] = VHost.new(name, @data_dir, @users, @replicator, description, tags)
              @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/)
            end
          end
          @replicator.try &.register_file(f)
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
      @replicator.try &.replace_file path
    end
  end
end
