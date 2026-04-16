require "./sortable_json"

module LavinMQ
  abstract class Queue
    include SortableJSON
    @name = ""

    def internal?
      false
    end
  end
end
