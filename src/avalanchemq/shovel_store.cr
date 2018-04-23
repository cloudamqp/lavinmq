require "./shovel"

module AvalancheMQ
  class ShovelStore
    @shovels = Hash(String, Shovel).new

    def create(name, config)
      src = Shovel::Source.new(config["src-uri"].as_s,
                               config["src-queue"].as_s?,
                               config["src-exchange"].as_s?,
                               config["src-exchange-key"].as_s?)
      dest = Shovel::Destination.new(config["dest-uri"].as_s,
                                     config["dest-queue"].as_s?,
                                     config["dest-exchange"].as_s?,
                                     config["dest-exchange-key"].as_s?)
      shovel = Shovel.new(src, dest)
      @shovels[name] = shovel
      spawn(name: "Shovel #{name}") { shovel.run }
      shovel
    end

    def delete(name)
      shovel = @shovel.delete name
      shovel.try &.stop
      shovel
    end
  end
end
