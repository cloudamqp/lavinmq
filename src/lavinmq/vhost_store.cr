require "json"
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

    def initialize(@data_dir : String, @users : Auth::UserStore, @replicator : Clustering::Replicator?, @persister : Persister)
      @vhosts = Hash(String, VHost).new
      @save_lock = Mutex.new
    end

    def []?(name : String) : VHost?
      @vhosts[name]?
    end

    def [](name : String) : VHost
      @vhosts[name]
    end

    def each_value(& : VHost ->) : Nil
      @vhosts.each_value { |v| yield v }
    end

    def has_key?(name : String) : Bool
      @vhosts.has_key?(name)
    end

    def size : Int32
      @vhosts.size
    end

    def values : Array(VHost)
      @vhosts.values
    end

    def first_value? : VHost?
      @vhosts.first_value?
    end

    def each(&)
      @vhosts.each do |kv|
        yield kv
      end
    end

    def create(name : String, user : Auth::BaseUser = @users.default_user, description = "", tags = Array(String).new(0), save : Bool = true)
      if v = @vhosts[name]?
        return v
      end
      vhost = VHost.new(name, @data_dir, @users, @replicator, @persister, description, tags)
      Log.info { "Created vhost #{name}" }
      # Grant the creating user full permissions on the new vhost. Only local
      # users have stored permissions; OAuth users get theirs from token scopes.
      if local_user = @users[user.name]?
        unless local_user.permissions[name]?
          @users.add_permission(user.name, name, /.*/, /.*/, /.*/, save: save)
        end
      end
      @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/, save: save)
      @vhosts[name] = vhost
      save! if save
      notify_observers(Event::Added, name)
      vhost
    end

    def delete(name) : VHost?
      if vhost = @vhosts.delete name
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
      # Stop outbound links (federation upstreams and shovels) on every vhost
      # before tearing any vhost down. A link holds an AMQP client connection to
      # another vhost, so force-closing one vhost's connections while another
      # vhost's link is still live would close that link's socket from under it —
      # which the amqp-client read loop logs as "connection closed unexpectedly".
      # Stopping them first lets each link close cleanly against a live peer.
      WaitGroup.wait do |wg|
        @vhosts.each_value do |vhost|
          wg.spawn do
            vhost.stop_shovels
            vhost.stop_upstream_links
          end
        end
      end
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

    def load!
      path = File.join(@data_dir, "vhosts.json")
      if File.exists? path
        Log.debug { "Loading vhosts from file" }
        File.open(path) do |f|
          JSON.parse(f).as_a.each do |vhost|
            name = vhost["name"].as_s
            tags = vhost["tags"]?.try(&.as_a.map(&.to_s)) || [] of String
            description = vhost["description"]?.try &.as_s || ""
            @vhosts[name] = VHost.new(name, @data_dir, @users, @replicator, @persister, description, tags)
            @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/)
          end
          @replicator.try &.register_file(f)
        end
      elsif Config.instance.load_definitions.empty?
        Log.debug { "Loading default vhosts" }
        create("/")
      end
      # Wait for vhosts to be loaded ("not closed")
      @vhosts.each_value &.closed.when_false.receive
      Log.debug { "#{size} vhosts loaded" }
    rescue ex
      Log.error(exception: ex) { "Failed to load vhosts" }
      raise ex
    end

    def save!
      Log.debug { "Saving vhosts to file" }
      path = File.join(@data_dir, "vhosts.json")
      # Serialize saves so concurrent create/delete don't race on the shared
      # `.tmp` file and fail the rename.
      @save_lock.synchronize do
        File.open("#{path}.tmp", "w") { |f| to_pretty_json(f); f.fsync }
        File.rename "#{path}.tmp", path
      end
      @replicator.try do |r|
        r.replace_file path
        # Block until a quorum has the new vhosts.json durably, so creating or
        # deleting a vhost is only acknowledged once it survives a leader failover
        # — otherwise the change lives only on the leader and is lost (the new
        # leader's full_sync deletes it) when the leader dies before followers
        # catch up. Mirrors the publish-confirm and definitions-store paths.
        r.wait_for_followers
      end
    end
  end
end
