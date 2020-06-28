require "json"
require "./vhost"

module AvalancheMQ
  class VHostStore
    include Enumerable({String, VHost})

    def initialize(@data_dir : String,
                   @log : Logger, @default_user : User)
      @vhosts = Hash(String, VHost).new
      load!
    end

    forward_missing_to @vhosts

    def each(&blk)
      @vhosts.each do |kv|
        yield kv
      end
    end

    def create(name, save = true)
      if v = @vhosts[name]?
        return v
      end
      vhost = VHost.new(name, @data_dir, @log.dup, @default_user)
      @vhosts[name] = vhost
      save! if save
      vhost
    end

    def delete(name) : VHost?
      if vhost = @vhosts.delete name
        vhost.delete
        save!
        vhost
      end
    ensure
      GC.collect
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
        @log.debug "Loading vhosts from file"
        File.open(path) do |f|
          JSON.parse(f).as_a.each do |vhost|
            next unless vhost.as_h?
            name = vhost["name"].as_s
            @vhosts[name] = VHost.new(name, @data_dir, @log.dup, @default_user)
          end
        rescue JSON::ParseException
          @log.warn("#{path} is not vaild json")
        end
      else
        @log.debug "Loading default vhosts"
        create("/")
      end
      @log.debug("#{size} vhosts loaded")
    end

    private def save!
      @log.debug "Saving vhosts to file"
      tmpfile = File.join(@data_dir, "vhosts.json.tmp")
      file = File.join(@data_dir, "vhosts.json")
      File.open(tmpfile, "w") { |f| to_pretty_json(f); f.fsync }
      File.rename tmpfile, file
    end
  end
end
