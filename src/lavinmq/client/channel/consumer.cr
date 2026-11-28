require "../../sortable_json"
require "../../bool_channel"

module LavinMQ
  abstract class Client
    abstract class Channel
      abstract class Consumer
        include SortableJSON
        @name = ""

        def ensure_deliver_loop; end

        # A channel that becomes ready when what keeps #accepts? false may
        # have changed, for waiting on capacity without polling. Nil when
        # nothing signals it.
        def accepts_signal : ::Channel(Nil)?
        end
      end
    end
  end
end
