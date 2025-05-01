require "./sortable_json"

module LavinMQ
  abstract class Queue
    include SortableJSON
    @name = ""
  end
end
