require "json"
require "./vhost"

module AvalancheMQ
  class VHostStore
    include Enumerable(VHost)

    def initialize(@data_dir : String, @log : Logger)
      @vhosts = Hash(String, VHost).new
      load!
    end

    def each
      @vhosts.values.each { |e| yield e }
    end

    def [](name)
      @vhosts[name]
    end

    def []?(name)
      @vhosts[name]?
    end

    def values
      @vhosts.values
    end

    def create(name, save = true)
      return if @vhosts.has_key?(name)
      vhost = VHost.new(name, @data_dir, @log)
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
    end

    def close
      @vhosts.each_value &.close
      save!
    end

    def to_json(json : JSON::Builder)
      @vhosts.values.to_json(json)
    end

    private def load!
      path = File.join(@data_dir, "vhosts.json")
      if File.exists? path
        @log.debug "Loading vhosts from file"
        File.open(path) do |f|
          JSON.parse(f).each do |vhost|
            next unless vhost.as_h?
            name = vhost["name"].as_s
            @vhosts[name] = VHost.new(name, @data_dir, @log)
          end
        end
      else
        @log.debug "Loading default vhosts"
        create("/", save: false)
        create("default", save: false)
        create("bunny_testbed", save: false)
        save!
      end
      @log.debug("#{@vhosts.size} vhosts loaded")
    end

    private def save!
      @log.debug "Saving vhosts to file"
      tmpfile = File.join(@data_dir, "vhosts.json.tmp")
      File.open(tmpfile, "w") { |f| self.to_pretty_json(f) }
      File.rename tmpfile, File.join(@data_dir, "vhosts.json")
    end
  end
end
