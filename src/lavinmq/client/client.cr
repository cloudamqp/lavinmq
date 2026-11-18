require "../sortable_json"
require "../rough_time"
require "./channel"

module LavinMQ
  abstract class Client
    include SortableJSON
    @name = ""
    @connected_at = RoughTime.unix_ms
    # Exclusive queues this connection declared, deleted when it closes
    @exclusive_queues = Array(AMQP::Queue).new

    abstract def run : Nil

    def queue_exclusive_to_other_client?(q : AMQP::Queue) : Bool
      q.exclusive? && !@exclusive_queues.includes?(q)
    end

    # Called by Queue#delete on the connection that declared an exclusive queue
    def exclusive_queue_deleted(q : AMQP::Queue) : Nil
      @exclusive_queues.delete(q)
    end

    private def close_exclusive_queues : Nil
      # Iterate a snapshot because Queue#close deletes exclusive queues,
      # which calls back into #exclusive_queue_deleted.
      @exclusive_queues.dup.each(&.close)
      @exclusive_queues.clear
    end

    private def duration : Time::Span
      ms = RoughTime.unix_ms - @connected_at
      Time::Span.new(seconds: (ms / 1000).round.to_i)
    end
  end
end
