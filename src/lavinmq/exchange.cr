require "./error"
require "./sortable_json"

module LavinMQ
  abstract class Exchange
    include SortableJSON
    @name = ""

    # Whether bindings whose source is this exchange should survive a restart.
    # Tracks durability for AMQP exchanges; the MQTT exchange overrides it to
    # true because it is always recreated at boot, so cross-protocol bindings
    # from it can be persisted and replayed (#1136).
    abstract def persistent? : Bool

    class AccessRefused < Error
      def initialize(@exchange : Exchange)
        super("Access refused to #{@exchange.name}")
      end
    end
  end
end
