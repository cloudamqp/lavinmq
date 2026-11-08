require "./error"
require "./sortable_json"

module LavinMQ
  abstract class Exchange
    include SortableJSON
    @name = ""

    class AccessRefused < Error
      def initialize(@exchange : Exchange, message : String? = nil)
        super(message || "Access refused to #{@exchange.name}")
      end
    end
  end
end
