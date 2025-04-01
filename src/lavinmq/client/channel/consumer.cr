require "../../sortable_json"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON
      end
    end
  end
end
