require "./error"
require "./sortable_json"

module LavinMQ
  abstract class Exchange
    include SortableJSON
    getter name = ""

    class AccessRefused < Error
      def initialize(exchange : Exchange)
        @name = exchange.name
        super("Access refused to #{exchange.name}")
      end

      def initialize(@name : String)
        super("Access refused to #{@name}")
      end
    end
  end
end
