require "./sortable_json"

module LavinMQ
  abstract class Queue
    include SortableJSON
  end
end
