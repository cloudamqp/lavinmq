require "../sortable_json"
require "./channel"

module LavinMQ
  abstract class Client
    include SortableJSON
  end
end
