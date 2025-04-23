require "../../sortable_json"
require "../../state_channel"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON
        @name = ""

        # Abstract method for accessing capacity state
        abstract def has_capacity : BoolChannel
      end
    end
  end
end
