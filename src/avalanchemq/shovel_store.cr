require "./shovel"

module AvalancheMQ
  class ShovelStore
    include Enumerable(Shovel)

    @shovels = Hash(String, Shovel).new

    def initialize(@vhost : VHost)
    end

    def each
      @shovels.values.each { |e| yield e }
    end

    def empty?
      @shovels.empty?
    end

    def create(name, config)
      delete_after_str = config["src-delete-after"]?.try &.as_s.delete("-")
      delete_after = if delete_after_str
                       Shovel::DeleteAfter.parse(delete_after_str)
                     else
                       Shovel::DeleteAfter::Never
                     end
      prefetch = config["src-prefetch-count"]?.try { |p| p.as_i.to_u16 } || 1000_u16
      src = Shovel::Source.new(config["src-uri"].as_s,
        config["src-queue"]?.try &.as_s?,
        config["src-exchange"]?.try &.as_s?,
        config["src-exchange-key"]?.try &.as_s?,
        delete_after,
        prefetch)
      dest = Shovel::Destination.new(config["dest-uri"].as_s,
        config["dest-queue"]?.try &.as_s?,
        config["dest-exchange"]?.try &.as_s?,
        config["dest-exchange-key"]?.try &.as_s?)
      shovel = Shovel.new(src, dest, name, @vhost)
      @shovels[name] = shovel
      spawn(name: "Shovel '#{name}'") { shovel.run }
      @vhost.log.info { "Shovel '#{name}' created" }
      shovel
    rescue KeyError
      raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
    end

    def delete(name)
      shovel = @shovels.delete name
      shovel.try &.stop
      @vhost.log.info { "Shovel '#{name}' deleted" }
      shovel
    end
  end
end
