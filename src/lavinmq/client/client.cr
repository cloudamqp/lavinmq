require "../sortable_json"
require "./channel"

module LavinMQ
  abstract class Client
    include SortableJSON
    @name = ""

    abstract def run : Nil
  end
end
