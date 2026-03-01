require "json"
require "./vhost"
require "./auth/base_user"

module LavinMQ
  class VHostStore
    include Enumerable({String, VHost})

    @on_added = Array(Proc(String, Nil)).new
    @on_deleted = Array(Proc(String, Nil)).new
    @on_closed = Array(Proc(String, Nil)).new

    def on_added(&block : String ->) : Proc(String, Nil)
      @on_added << block
      block
    end

    def on_deleted(&block : String ->) : Proc(String, Nil)
      @on_deleted << block
      block
    end

    def on_closed(&block : String ->) : Proc(String, Nil)
      @on_closed << block
      block
    end

    Log = LavinMQ::Log.for "vhost_store"

    def initialize(@data_dir : String, @users : Auth::UserStore, @replicator : Clustering::Replicator?)
      @vhosts = Hash(String, VHost).new
    end

    forward_missing_to @vhosts

    def each(&)
      @vhosts.each do |kv|
        yield kv
      end
    end

    def create(name : String, user : Auth::BaseUser = @users.default_user, description = "", tags = Array(String).new(0), save : Bool = true)
      if v = @vhosts[name]?
        return v
      end
      vhost = VHost.new(name, @data_dir, @users, @replicator, description, tags)
      Log.info { "Created vhost #{name}" }
      @users.add_permission(user.name, name, /.*/, /.*/, /.*/)
      @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/)
      @vhosts[name] = vhost
      save! if save
      @on_added.dup.each &.call(name)
      vhost
    end

    def delete(name) : VHost?
      if vhost = @vhosts.delete name
        @users.rm_vhost_permissions_for_all(name)
        vhost.delete
        @on_deleted.dup.each &.call(name)
        Log.info { "Deleted vhost #{name}" }
        save!
        vhost
      end
    end

    def close
      WaitGroup.wait do |wg|
        @vhosts.each_value do |vhost|
          wg.spawn do
            vhost.close
            @on_closed.dup.each &.call(vhost.name)
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
            @vhosts[name] = VHost.new(name, @data_dir, @users, @replicator, description, tags)
            @users.add_permission(@users.direct_user, name, /.*/, /.*/, /.*/)
          end
          @replicator.try &.register_file(f)
        end
      else
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

    private def save!
      Log.debug { "Saving vhosts to file" }
      path = File.join(@data_dir, "vhosts.json")
      File.open("#{path}.tmp", "w") { |f| to_pretty_json(f); f.fsync }
      File.rename "#{path}.tmp", path
      @replicator.try &.replace_file path
    end
  end
end
