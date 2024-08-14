require "log"
require "../../sortable_json"
require "../../error"
require "../../logger"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON
      end
    end
  end
end
