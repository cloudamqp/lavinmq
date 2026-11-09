require "./error"
require "./sortable_json"

module LavinMQ
  abstract class Exchange
    include SortableJSON
    @name = ""

    class AccessRefused < Error
      def initialize(exchange : Exchange, message : String? = nil)
        super(message || "Access refused to #{exchange.name}")
      end

      # For a declare, which is refused before the exchange exists
      def initialize(message : String)
        super(message)
      end
    end
  end
end
