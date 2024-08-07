require "../sortable_json"
require "./channel/consumer"

module LavinMQ
  abstract class Client
    abstract class Channel
      include SortableJSON
    end
  end
end
