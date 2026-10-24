require "../sortable_json"
require "./channel"

module LavinMQ
  abstract class Client
    include SortableJSON
    @name = ""

    abstract def run : Nil

    # Called by Queue#delete on the connection that declared an exclusive queue
    def exclusive_queue_deleted(q) : Nil
    end
  end
end
