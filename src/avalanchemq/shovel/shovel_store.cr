require "./shovel"

module AvalancheMQ
  class ShovelStore
    include Enumerable({String, Shovel})
    Log = ::Log.for(self)

    def initialize(@vhost : VHost)
      @shovels = Hash(String, Shovel).new
    end

    forward_missing_to @shovels

    def create(name, config)
      delete(name)
      delete_after_str = config["src-delete-after"]?.try(&.as_s.delete("-")).to_s
      delete_after = Shovel::DeleteAfter.parse?(delete_after_str) || Shovel::DEFUALT_DELETE_AFTER
      ack_mode_str = config["ack-mode"]?.try(&.as_s.delete("-")).to_s
      ack_mode = Shovel::AckMode.parse?(ack_mode_str) || Shovel::DEFAULT_ACK_MODE
      reconnect_delay = config["reconnect-delay"]?.try &.as_i || Shovel::DEFUALT_RECONNECT_DELAY
      prefetch = config["src-prefetch-count"]?.try { |p| p.as_i.to_u16 } || Shovel::DEFAULT_PREFETCH
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
      shovel = Shovel.new(src, dest, name, @vhost, ack_mode, reconnect_delay)
      @shovels[name] = shovel
      spawn(shovel.run, name: "Shovel '#{name}'")
      shovel
    rescue KeyError
      raise JSON::Error.new("Fields 'src-uri' and 'dest-uri' are required")
    end

    def delete(name)
      if shovel = @shovels.delete name
        shovel.stop
        Log.info { "Shovel '#{name}' deleted" }
        shovel
      end
    end

    def each
      @shovels.each_value do |v|
        yield v
      end
    end
  end
end
