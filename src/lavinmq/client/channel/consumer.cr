require "../../sortable_json"
require "../../bool_channel"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON
        @name = ""
      end
    end
  end
end
