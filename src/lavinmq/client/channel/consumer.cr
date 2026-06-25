require "../../sortable_json"
require "../../bool_channel"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON
        @name = ""

        def ensure_deliver_loop; end
      end
    end
  end
end
