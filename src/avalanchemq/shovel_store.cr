require "./shovel"

module AvalancheMQ
  class ShovelStore
    include Enumerable(Shovel)

    @shovels = Hash(String, Shovel).new

    def each
      @shovels.values.each { |e| yield e }
    end

    def create(name, config)
      src = Shovel::Source.new(config["src-uri"].as_s,
                               config["src-queue"]?.try &.as_s?,
                               config["src-exchange"]?.try &.as_s?,
                               config["src-exchange-key"]?.try &.as_s?)
      dest = Shovel::Destination.new(config["dest-uri"].as_s,
                                     config["dest-queue"]?.try &.as_s?,
                                     config["dest-exchange"]?.try &.as_s?,
                                     config["dest-exchange-key"]?.try &.as_s?)
      shovel = Shovel.new(src, dest)
      @shovels[name] = shovel
      spawn(name: "Shovel #{name}") { shovel.run }
      shovel
    rescue KeyError
      raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
    end

    def delete(name)
      shovel = @shovels.delete name
      shovel.try &.stop
      shovel
    end
  end
end
