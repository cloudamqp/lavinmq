require "./constants"
require "../endpoint/session"

module LavinMQ
  module Shovel
    # A Source yields messages to the Runner and settles them on demand.
    # The Runner — not the Destination — decides whether a delivery is acked
    # or requeued, by calling #ack or #reject after a Destination reports its
    # Outcome. See Runner#run.
    abstract class Source
      abstract def start
      abstract def stop

      # True while the source can settle deliveries. Outcomes that arrive for a
      # stopped source (confirms voided by a destination close during pause or
      # terminate) have nothing to act on and are ignored by the Runner.
      abstract def started? : Bool

      # Yields each consumed message. Returns when the source is exhausted
      # (delete-after) or stopped.
      #
      # The delivery's body and properties are borrowed (see
      # Endpoint::Delivery): valid only until the block returns.
      abstract def each(&blk : Endpoint::Delivery -> Nil)

      # Acknowledge a successfully shoveled message.
      abstract def ack(delivery_tag)

      # Return a message to the source. With requeue: true it stays available
      # for redelivery; with requeue: false it is dropped/dead-lettered.
      abstract def reject(delivery_tag, requeue)

      abstract def delete_after
    end
  end
end
