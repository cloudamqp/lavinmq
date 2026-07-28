require "amqp-client"
require "./constants"

module LavinMQ
  module Shovel
    # A Source yields messages to the Runner and settles them on demand.
    # The Runner — not the Destination — decides whether a delivery is acked
    # or requeued, by calling #ack or #reject after a Destination reports its
    # Outcome. See Runner#run.
    abstract class Source
      abstract def start
      abstract def stop

      # Yields each consumed message. Returns when the source is exhausted
      # (delete-after) or stopped.
      abstract def each(&blk : ::AMQP::Client::DeliverMessage -> Nil)

      # Acknowledge a successfully shoveled message.
      abstract def ack(delivery_tag)

      # Return a message to the source. With requeue: true it stays available
      # for redelivery; with requeue: false it is dropped/dead-lettered.
      abstract def reject(delivery_tag, requeue)

      abstract def delete_after
    end
  end
end
