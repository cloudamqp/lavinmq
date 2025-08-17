require "../sortable_json"
require "./channel/consumer"

module LavinMQ
  abstract class Client
    abstract class Channel
      include SortableJSON
      @name = ""

      def consumer_count : Int32
        0
      end
    end
  end
end
