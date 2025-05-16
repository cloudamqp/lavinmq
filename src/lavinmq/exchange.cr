require "./destination"
require "./error"
require "./sortable_json"

module LavinMQ
  abstract class Exchange
    include SortableJSON
    @name = ""

    class AccessRefused < Error
      def initialize(@exchange : Exchange)
        super("Access refused to #{@exchange.name}")
      end
    end
  end
end
